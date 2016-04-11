from __future__ import unicode_literals, print_function

import argparse
import json
import logging
import sys

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

    def __init__(self, buffering_time):
        super(CollectorStream, self).__init__('collector',
                                label='semserver-collector',
                                num_recent_events=16384,
                                persist_events=True,
                                parse_event_body=True,
                                buffering_time=buffering_time,
                                allow_publish=True,
                                custom_publish_handler=PublishRequestHandler)
        self.latest_locations = utils.LatestValueBuffer()
        self.timers = [
            tornado.ioloop.PeriodicCallback(self._roll_latest_locations,
                                            self.ROLL_LOCATIONS_PERIOD,
                                            io_loop=self.ioloop),
        ]

    def start(self):
        super(CollectorStream, self).start()
        for timer in self.timers:
            timer.start()

    def stop(self):
        super(CollectorStream, self).stop()
        for timer in self.timers:
            timer.stop()

    def _roll_latest_locations(self):
        logging.info('Roll latest locations buffer')
        self.latest_locations.roll()


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
    DISTANCE_THR = 10.0

    def __init__(self, application, request, **kwargs):
        super(PublishRequestHandler, self).__init__(application,
                                                    request,
                                                    **kwargs)
        self.set_response_timeout(self.TIMEOUT)
        self.feedback = feedback.DriverFeedback()
        self.pending_pieces = 2

    @tornado.web.asynchronous
    def get(self):
        self.post()

    @tornado.web.asynchronous
    def post(self):
        events = self.get_and_dispatch_events(finish_request=False)
        if (events and events[0].application_id == 'SmartDriver'
            and events[0].event_type == 'Vehicle Location'):
            location = locations.Location( \
                                    events[0].body['Location']['latitude'],
                                    events[0].body['Location']['longitude'])
            user_id = events[0].source_id
            self._request_road_info(user_id, location)
            self._request_scores(user_id, location)
        else:
            self.finish()

    def on_response_timeout(self):
        # Respond anyway
        self.respond()

    def respond(self):
        if not self.finished:
            data = utils.serialize_object_json(self.feedback, compress=True)
            logging.info(self.feedback.as_dict())
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

    def _request_road_info(self, user_id, location):
        try:
            previous = self.stream.latest_locations[user_id]
        except KeyError:
            logging.info('No previous location for {}'.format(user_id[:12]))
            self._get_road_info(location, location)
            self.stream.latest_locations[user_id] = location
        else:
            if location.distance(previous) >= self.DISTANCE_THR:
                self._get_road_info(location, previous)
                self.stream.latest_locations[user_id] = location
            else:
                self.stream.latest_locations.refresh(user_id)
                logging.info('Location too close to the previous one')
        self._end_of_piece()

    def _request_scores(self, user_id, location):
        feedback.fake_scores(self.feedback, base=location)
        self._end_of_piece()

    @tornado.gen.coroutine
    def _get_road_info(self, current_location, previous_location):
        # Send the request
        params = {
            'currentLat': current_location.lat,
            'currentLong': current_location.long,
            'previousLat': previous_location.lat,
            'previousLong': previous_location.long,
        }
        url = tornado.httputil.url_concat(self.ROAD_INFO_URL, params)
        logging.info(url)
        client = tornado.httpclient.AsyncHTTPClient()
        request = tornado.httpclient.HTTPRequest(url,
                                                 connect_timeout=3.8,
                                                 request_timeout=4.0)
        try:
            response = yield client.fetch(request)
            if response.code == 200 and response.body:
                data = json.loads(response.body)
                logging.info(data)
                self.feedback.road_info = {
                    'roadType': data['linkType'],
                    'maxSpeed': data['maxSpeed'],
                }
            else:
                logging.info('No response received')
        except:
            logging.warning('Exception while sending HTTP request',
                            exc_info=sys.exc_info())


def _read_cmd_arguments():
    parser = argparse.ArgumentParser( \
                    description='Run the HERMES collector server.')
    utils.add_server_options(parser, 9100)
    args = parser.parse_args()
    return args


def _create_stream_server(port, buffering_time):
    server = ztreamy.StreamServer(port)
    collector_stream = CollectorStream(buffering_time)
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
    server, type_relays = _create_stream_server(args.port, buffering_time)
    try:
        type_relays.start()
        server.start()
    except KeyboardInterrupt:
        pass
    finally:
        server.stop()


if __name__ == "__main__":
    main()
