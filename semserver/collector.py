from __future__ import unicode_literals, print_function

import argparse
import json
import logging
import os

import tornado.web
import tornado.gen
import tornado.httpclient
import tornado.httputil
import ztreamy
import ztreamy.server

from . import utils
from . import feedback
from . import locations


class CollectorStream(ztreamy.Stream):
    ROLL_LOCATIONS_PERIOD = 60000 # 1 minute

    def __init__(self, buffering_time, disable_feedback=False,
                 disable_persistence=False):
        super(CollectorStream, self).__init__('collector',
                                label='semserver-collector',
                                num_recent_events=16384,
                                persist_events=not disable_persistence,
                                parse_event_body=True,
                                buffering_time=buffering_time,
                                allow_publish=True,
                                custom_publish_handler=PublishRequestHandler)
        self.latest_locations = utils.LatestValueBuffer()
        self.timers = [
            tornado.ioloop.PeriodicCallback(self._roll_latest_locations,
                                            self.ROLL_LOCATIONS_PERIOD,
                                            io_loop=self.ioloop),
            tornado.ioloop.PeriodicCallback(self._periodic_stats,
                                            30000,
                                            io_loop=self.ioloop),
        ]
        self.disable_feedback = disable_feedback
        self.num_events = 0
        self.latest_times = os.times()

    def start(self):
        super(CollectorStream, self).start()
        for timer in self.timers:
            timer.start()

    def stop(self):
        super(CollectorStream, self).stop()
        for timer in self.timers:
            timer.stop()

    def count_events(self, num_events):
        self.num_events += num_events

    def _roll_latest_locations(self):
        logging.debug('Roll latest locations buffer')
        self.latest_locations.roll()

    def _periodic_stats(self):
        logging.warn('Events in the last 30s: {}'.format(self.num_events))
        self.num_events = 0
        current_times = os.times()
        user_time = current_times[0] - self.latest_times[0]
        sys_time = current_times[1] - self.latest_times[1]
        total_time = user_time + sys_time
        self.latest_times = current_times
        logging.warn('Time: {} = {} + {}'.format(total_time, user_time,
                                                 sys_time))


class EventTypeRelays(ztreamy.LocalClient):
    def __init__(self, stream, application_id, event_types, buffering_time):
        super(EventTypeRelays, self).__init__(stream, self.process_event)
        self.application_id = application_id
        self.relays = {}
        for event_type in event_types:
            path = 'collector/type/' + event_type.replace(' ', '')
            self.relays[event_type] = ztreamy.Stream( \
                                            path,
                                            buffering_time=buffering_time,
                                            allow_publish=False)

    def process_event(self, event):
        if (event.application_id == self.application_id
            and event.event_type in self.relays):
            self.relays[event.event_type].dispatch_event(event)

    def start(self):
        super(EventTypeRelays, self).start()


class PublishRequestHandler(ztreamy.server.EventPublishHandlerAsync):
    TIMEOUT = 5.0
    ROAD_INFO_URL = ('http://cronos.lbd.org.es'
                     '/hermes/api/smartdriver/network/link')
    SCORE_INFO_URL = 'http://localhost:9101/driver_scores'
    DISTANCE_THR = 10.0

    def __init__(self, application, request, **kwargs):
        super(PublishRequestHandler, self).__init__(application,
                                                    request,
                                                    **kwargs)
        if not self.stream.disable_feedback:
            self.set_response_timeout(self.TIMEOUT)
            self.feedback = feedback.DriverFeedback()
            self.pending_pieces = 2

    @tornado.web.asynchronous
    def get(self):
        self.post()

    @tornado.web.asynchronous
    def post(self):
        events = self.get_and_dispatch_events(finish_request=False)
        self.stream.count_events(len(events))
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

    def on_response_timeout(self):
        # Respond anyway
        if not self.finished:
            logging.warning('Publish timeout, responding to the request')
            if self.feedback.road_info.status is None:
                self.feedback.no_data(feedback.Status.SERVICE_TIMEOUT)
            self.respond()

    def respond(self):
        if not self.finished:
            data = utils.serialize_object_json(self.feedback, compress=True)
            self.set_header('Content-Type', ztreamy.json_media_type)
            self.set_header('Content-Encoding', 'gzip')
            self.write(data)
            self.finish()

    def _try_to_respond(self):
        if self.pending_pieces == 0:
            self.respond()

    def _end_of_piece(self):
        self.pending_pieces -= 1
        self._try_to_respond()

    @tornado.gen.coroutine
    def _request_info(self, user_id, location, score):
        try:
            previous = self.stream.latest_locations[user_id]
        except KeyError:
            logging.debug('No previous location for {}'.format(user_id[:12]))
            self.stream.latest_locations[user_id] = location
            self._request_road_info(location, location)
            self._request_scores(user_id, location, score)
        else:
            if location.distance(previous) >= self.DISTANCE_THR:
                self.stream.latest_locations[user_id] = location
                self._request_road_info(location, previous)
                self._request_scores(user_id, location, score)
            else:
                self.stream.latest_locations.refresh(user_id)
                self.feedback.no_data(feedback.Status.USE_PREVIOUS)
                logging.debug('Location too close to the previous one')
                self._end_of_piece()
                self._end_of_piece()

    @tornado.gen.coroutine
    def _request_road_info(self, current_location, previous_location):
        # Send the request
        params = {
            'currentLat': current_location.lat,
            'currentLong': current_location.long,
            'previousLat': previous_location.lat,
            'previousLong': previous_location.long,
        }
        url = tornado.httputil.url_concat(self.ROAD_INFO_URL, params)
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
        self._end_of_piece()

    @tornado.gen.coroutine
    def _request_scores(self, user_id, location, score):
        ## feedback.fake_scores(self.feedback, base=location)
        params = {
            'user': user_id,
            'latitude': location.lat,
            'longitude': location.long,
            'score': score,
        }
        url = tornado.httputil.url_concat(self.SCORE_INFO_URL, params)
        logging.debug(url)
        client = tornado.httpclient.AsyncHTTPClient()
        request = tornado.httpclient.HTTPRequest(url,
                                                 request_timeout=self.TIMEOUT)
        try:
            response = yield client.fetch(request)
            if response.code == 200:
                self.feedback.scores.load_from_csv(response.body)
                logging.debug('Received {} scores'\
                             .format(len(self.feedback.scores.scores)))
            else:
                self.feedback.scores.no_data(feedback.Status.SERVICE_ERROR)
                logging.warning('Error status code in scores request: {}'\
                                .format(response.code))
        except Exception as e:
            self.feedback.scores.no_data(feedback.Status.SERVICE_ERROR)
            logging.warning(e)
        self._end_of_piece()


def _read_cmd_arguments():
    parser = argparse.ArgumentParser( \
                    description='Run the HERMES collector server.')
    parser.add_argument('--disable-feedback', dest='disable_feedback',
                        action='store_true')
    parser.add_argument('--disable-persistence', dest='disable_persistence',
                        action='store_true')
    utils.add_server_options(parser, 9100)
    args = parser.parse_args()
    return args


def _create_stream_server(port, buffering_time, disable_feedback=False,
                          disable_persistence=False):
    server = ztreamy.StreamServer(port)
    collector_stream = CollectorStream(buffering_time,
                                       disable_feedback=disable_feedback,
                                       disable_persistence=disable_persistence)
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
    server.add_stream(collector_stream)
    for stream in type_relays.relays.values():
        server.add_stream(stream)
    return server, type_relays

def main():
    args = _read_cmd_arguments()
    if args.buffer > 0:
        buffering_time = args.buffer * 1000
    else:
        buffering_time = None
    utils.configure_logging('collector')
    server, type_relays = _create_stream_server( \
                                args.port,
                                buffering_time,
                                disable_feedback=args.disable_feedback,
                                disable_persistence=args.disable_persistence)
    try:
        type_relays.start()
        server.start()
    except KeyboardInterrupt:
        pass
    finally:
        server.stop()


if __name__ == "__main__":
    main()
