from __future__ import unicode_literals, print_function

import argparse
import json
import logging
import datetime

import tornado.web
import tornado.gen
import tornado.httpclient
import tornado.httputil
import ztreamy
import ztreamy.server
import ztreamy.client

from . import utils
from . import feedback
from . import locations


DEFAULT_ROAD_INFO_URL = ('http://cronos.lbd.org.es'
                         '/hermes/api/smartdriver/network/link')
DEFAULT_SCORE_INFO_URL = 'http://localhost:9101/driver_scores'


class CollectorStream(ztreamy.Stream):
    ROLL_LOCATIONS_PERIOD = 60000 # 1 minute
    THRESHOLD_DISTANCE = 10.0

    def __init__(self, buffering_time,
                 label='collector',
                 ioloop=None,
                 disable_feedback=False,
                 disable_persistence=False,
                 disable_road_info=False,
                 backend_stream=None,
                 score_info_url=DEFAULT_SCORE_INFO_URL,
                 road_info_url=DEFAULT_SCORE_INFO_URL,
                 log_event_time=None):
        super(CollectorStream, self).__init__('collector',
                                label=label,
                                num_recent_events=2**16,
                                persist_events=not disable_persistence,
                                parse_event_body=True,
                                buffering_time=buffering_time,
                                allow_publish=True,
                                custom_publish_handler=PublishRequestHandler,
                                ioloop=ioloop)
        self.latest_locations = LatestLocationsBuffer(self.THRESHOLD_DISTANCE)
        self.timers = [
            tornado.ioloop.PeriodicCallback(self._roll_latest_locations,
                                            self.ROLL_LOCATIONS_PERIOD,
                                            io_loop=self.ioloop),
            ## tornado.ioloop.PeriodicCallback(self._periodic_stats,
            ##                                 60000,
            ##                                 io_loop=self.ioloop),
        ]
        self.disable_feedback = disable_feedback
        self.disable_road_info = disable_road_info
        self.score_info_url = score_info_url
        self.road_info_url = road_info_url
        self.stats_tracker = utils.StatsTracker(self)
        self.events_tracker = utils.EventsTracker.create(log_event_time, self)
        if backend_stream:
            self.backend_relay = BackendStreamRelay(self, backend_stream, 0.1,
                                                    ioloop=self.ioloop)
        else:
            self.backend_relay = None
        self._schedule_next_stats_period()

    def start(self):
        super(CollectorStream, self).start()
        if self.backend_relay is not None:
            self.backend_relay.start()
        for timer in self.timers:
            timer.start()

    def stop(self):
        if self.backend_relay is not None:
            self.backend_relay.stop()
        for timer in self.timers:
            timer.stop()
        super(CollectorStream, self).stop()

    def _roll_latest_locations(self):
        logging.debug('Roll latest locations buffer')
        self.latest_locations.roll()

    def _periodic_stats(self):
        utils.log_stats_value(self.label, self.stats_tracker.compute_cycle())
        self.events_tracker.log()
        self._schedule_next_stats_period()

    def _schedule_next_stats_period(self):
        self.ioloop.add_timeout(datetime.timedelta( \
                                        seconds=60 - self.ioloop.time() % 60),
                                self._periodic_stats)


class BackendStreamRelay(ztreamy.LocalClient):
    def __init__(self, stream, backend_stream_url, buffering_time,
                 ioloop=None):
        super(BackendStreamRelay, self).__init__(stream,
                                                 self.process_events,
                                                 separate_events=False)
        self.publisher = ztreamy.client.ContinuousEventPublisher( \
                                    backend_stream_url,
                                    buffering_time=buffering_time,
                                    io_loop=ioloop)
        logging.info('Connected to backend stream {}'
                     .format(backend_stream_url))

    def process_events(self, events):
        self.publisher.publish_events(events)

    def start(self):
        super(BackendStreamRelay, self).start()
        self.publisher.start()

    def stop(self):
        super(BackendStreamRelay, self).stop()
        self.publisher.stop()


class EventTypeRelays(ztreamy.LocalClient):
    def __init__(self, stream, application_id, event_types, buffering_time,
                 ioloop=None):
        super(EventTypeRelays, self).__init__(stream, self.process_event)
        self.application_id = application_id
        self.relays = {}
        for event_type in event_types:
            path = stream.path + '/type/' + event_type.replace(' ', '')
            self.relays[event_type] = ztreamy.Stream( \
                                            path,
                                            buffering_time=buffering_time,
                                            allow_publish=False,
                                            ioloop=ioloop)

    def process_event(self, event):
        if (event.application_id == self.application_id
            and event.event_type in self.relays):
            self.relays[event.event_type].dispatch_event(event)

    def start(self):
        super(EventTypeRelays, self).start()


class PublishRequestHandler(ztreamy.server.EventPublishHandlerAsync):
    TIMEOUT = 5.0
    DISTANCE_THR = 10.0

    def __init__(self, application, request, **kwargs):
        super(PublishRequestHandler, self).__init__(application,
                                                    request,
                                                    **kwargs)
        if not self.stream.disable_feedback:
            self.set_response_timeout(self.TIMEOUT)
            self.feedback = feedback.DriverFeedback()
            self.previous_location = None

    @tornado.web.asynchronous
    def get(self):
        self.post()

    @tornado.web.asynchronous
    def post(self):
        if self.stream.running:
            events = self.get_and_dispatch_events(finish_request=False)
            self.stream.events_tracker.track_events(events)
            if (not self.stream.disable_feedback
                and events
                and events[0].application_id == 'SmartDriver'
                and events[0].event_type == 'Vehicle Location'):
                location = locations.Location( \
                                    events[0].body['Location']['latitude'],
                                    events[0].body['Location']['longitude'])
                score = events[0].body['Location']['score']
                user_id = events[0].source_id
                self._request_info(user_id, location, score)
            else:
                self.finish()
        else:
            raise tornado.web.HTTPError(503, 'The stream is stopped')

    def on_response_timeout(self):
        # Respond anyway
        if not self.finished:
            self.feedback.timeout()
            logging.warning('Publish timeout: {}/{}'\
                            .format(self.feedback.scores.status,
                                    self.feedback.road_info.status))
            self.respond()

    def respond(self):
        if not self.finished:
            data = utils.serialize_object_json(self.feedback, compress=True)
            logging.debug('Finish request')
            logging.debug(self.feedback.as_dict())
            self.set_header('Content-Type', ztreamy.json_media_type)
            self.set_header('Content-Encoding', 'gzip')
            self.write(data)
            self.finish()

    @tornado.gen.coroutine
    def _request_info(self, user_id, location, score):
        if self.stream.latest_locations.check(user_id, location):
            yield self._request_scores(user_id, location, score)
            if self.previous_location is not None:
                yield self._request_road_info(location, self.previous_location)
            else:
                self.feedback.road_info.no_data(self.feedback.scores.status)
        else:
            self.feedback.no_data(feedback.Status.USE_PREVIOUS)
        self.respond()

    @tornado.gen.coroutine
    def _request_road_info(self, current_location, previous_location):
        if self.stream.disable_road_info:
            self.feedback.road_info.no_data(feedback.Status.DISABLED)
            return
        # Send the request
        params = {
            'currentLat': current_location.lat,
            'currentLong': current_location.long,
            'previousLat': previous_location.lat,
            'previousLong': previous_location.long,
        }
        url = tornado.httputil.url_concat(self.stream.road_info_url, params)
        logging.debug(url)
        client = tornado.httpclient.AsyncHTTPClient()
        request = tornado.httpclient.HTTPRequest(url,
                                                 request_timeout=self.TIMEOUT)
        try:
            response = yield client.fetch(request)
            if response.code == 200:
                if response.body:
                    data = json.loads(response.body)
                    logging.debug(data)
                    self.feedback.road_info.set_data(data['linkType'],
                                                     data['maxSpeed'])
                else:
                    logging.debug('No data available')
                    self.feedback.road_info.no_data(feedback.Status.NO_DATA)
            else:
                self.feedback.road_info.no_data(feedback.Status.SERVICE_ERROR)
        except:
            self.feedback.road_info.no_data(feedback.Status.SERVICE_ERROR)

    @tornado.gen.coroutine
    def _request_scores(self, user_id, location, score):
        ## feedback.fake_scores(self.feedback, base=location)
        params = {
            'user': user_id,
            'latitude': location.lat,
            'longitude': location.long,
            'score': score,
        }
        url = tornado.httputil.url_concat(self.stream.score_info_url, params)
        logging.debug(url)
        client = tornado.httpclient.AsyncHTTPClient()
        request = tornado.httpclient.HTTPRequest(url,
                                                 request_timeout=self.TIMEOUT)
        try:
            response = yield client.fetch(request)
            if response.code == 200:
                lines = response.body.split('\r\n')
                if lines:
                    if lines[0].startswith('#+'):
                        self.previous_location = \
                                        locations.Location.parse(lines[0][2:])
                        self.feedback.scores.load_from_lines(lines[1:])
                        logging.debug('Received {} scores'\
                                    .format(len(self.feedback.scores.scores)))
                    else:
                        self.feedback.scores.no_data( \
                                                feedback.Status.USE_PREVIOUS)
                        if lines[0].startswith('#i'):
                            self.previous_location = \
                                        locations.Location.parse(lines[0][2:])
            else:
                logging.warning('Error status code in scores request: {}'\
                                .format(response.code))
        except Exception as e:
            logging.warning(e)
        if self.feedback.scores.status is None:
            self.feedback.scores.no_data(feedback.Status.SERVICE_ERROR)


class LatestLocationsBuffer(utils.LatestValueBuffer):
    def __init__(self, threshold_distance):
        super(LatestLocationsBuffer, self).__init__()
        self.threshold_distance = threshold_distance

    def check(self, user_id, location):
        try:
            previous = self[user_id]
        except KeyError:
            answer = True
        else:
            ## logging.debug('d {} / {} / {}'.format(location.distance(previous),
            ##                                       location,
            ##                                       previous))
            if location.distance(previous) >= self.threshold_distance:
                answer = True
            else:
                answer = False
        if answer:
            self[user_id] = location
        else:
            self.refresh(user_id)
        return answer


def read_cmd_arguments(default_port=9100, is_frontend_server=False):
    if is_frontend_server:
        default_backend_stream = 'http://localhost:9109/backend/'
    else:
        default_backend_stream = None
    parser = argparse.ArgumentParser( \
                    description='Run the HERMES collector server.')
    parser.add_argument('--disable-feedback', dest='disable_feedback',
                        action='store_true')
    parser.add_argument('--disable-persistence', dest='disable_persistence',
                        action='store_true')
    parser.add_argument('--disable-road-info', dest='disable_road_info',
                        action='store_true')
    parser.add_argument('-k', '--backend-stream', dest='backend_stream',
                        default=default_backend_stream,
                        help='Backend stream URL')
    parser.add_argument('-i', '--score-info-url', dest='score_info_url',
                        default=DEFAULT_SCORE_INFO_URL,
                        help='Scores info service URL')
    parser.add_argument('-r', '--road-info-url', dest='road_info_url',
                        default=DEFAULT_ROAD_INFO_URL,
                        help='Road info service URL')
    parser.add_argument('-l', '--log-event-time', dest='log_event_time',
                        default=None,
                        help=('Log event arrival time ("all", '
                              '"0", "00", "000", etc.'))
    utils.add_server_options(parser, default_port, stream=True)
    args = parser.parse_args()
    return args


def _create_stream_server(port, buffering_time, disable_feedback=False,
                          disable_road_info=False,
                          disable_persistence=False,
                          backend_stream=None,
                          score_info_url=DEFAULT_SCORE_INFO_URL,
                          road_info_url=DEFAULT_ROAD_INFO_URL,
                          log_event_time=None):
    server = ztreamy.StreamServer(port)
    collector_stream = CollectorStream(buffering_time,
                                       disable_feedback=disable_feedback,
                                       disable_road_info=disable_road_info,
                                       disable_persistence=disable_persistence,
                                       backend_stream=backend_stream,
                                       score_info_url=score_info_url,
                                       road_info_url=road_info_url,
                                       log_event_time=log_event_time)
    if not backend_stream:
        type_relays = EventTypeRelays(collector_stream,
                                      'SmartDriver',
                                      ['Vehicle Location',
                                       'High Speed',
                                       'High Acceleration',
                                       'High Deceleration',
                                       'High Heart Rate',
                                       'Data Section',
                                       'Context Data',
                                      ],
                                      buffering_time)
    else:
        type_relays = None
    server.add_stream(collector_stream)
    if type_relays:
        for stream in type_relays.relays.values():
            server.add_stream(stream)
    return server

def main():
    args = read_cmd_arguments()
    if args.buffer > 0:
        buffering_time = args.buffer * 1000
    else:
        buffering_time = None
    utils.configure_logging('collector', level=args.log_level,
                            disable_stderr=args.disable_stderr)
    server = _create_stream_server( \
                                args.port,
                                buffering_time,
                                disable_feedback=args.disable_feedback,
                                disable_road_info=args.disable_road_info,
                                disable_persistence=args.disable_persistence,
                                backend_stream=args.backend_stream,
                                score_info_url=args.score_info_url,
                                road_info_url=args.road_info_url,
                                log_event_time=args.log_event_time)
    ztreamy.client.configure_max_clients(1000)
    try:
        server.start()
    except KeyboardInterrupt:
        pass
    finally:
        server.stop()


if __name__ == "__main__":
    main()
