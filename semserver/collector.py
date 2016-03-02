from __future__ import unicode_literals, print_function

import json
import argparse

import tornado
import ztreamy
import ztreamy.server

from . import utils


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
    def __init__(self, application, request, **kwargs):
        super(PublishRequestHandler, self).__init__(application,
                                                    request,
                                                    **kwargs)
        self.set_response_timeout(5.0)

    @tornado.web.asynchronous
    def get(self):
        self.post()

    @tornado.web.asynchronous
    def post(self):
        events = self.get_and_dispatch_events(finish_request=False)
        if events and events[0].application_id == 'SmartDriver':
            self.ioloop.call_later(1.0, self.respond)
        else:
            self.finish()

    def respond(self):
        if not self.finished:
            answer = {'test': 0.0}
            self.set_header('Content-Type', ztreamy.json_media_type)
            self.write(json.dumps(answer))
            self.finish()


def _read_cmd_arguments():
    parser = argparse.ArgumentParser( \
                    description='Run the HERMES collector server.')
    utils.add_server_options(parser, 9100)
    args = parser.parse_args()
    return args

def main():
    args = _read_cmd_arguments()
    if args.buffer > 0:
        buffering_time = args.buffer * 1000
    else:
        buffering_time = None
    utils.configure_logging('collector')
    server = ztreamy.StreamServer(args.port)
    collector_stream = ztreamy.Stream('collector',
                                label='semserver-collector',
                                num_recent_events=16384,
                                persist_events=True,
                                parse_event_body=True,
                                buffering_time=buffering_time,
                                allow_publish=True,
                                custom_publish_handler=PublishRequestHandler)
    type_relays = EventTypeRelays(collector_stream,
                                  'SmartDriver',
                                  ['Vehicle Location',
                                   'High Speed',
                                   'High Acceleration',
                                   'High Deceleration',
                                   'High Heart Rate',
                                   'Data Section'],
                                  buffering_time)
    server.add_stream(collector_stream)
    for stream in type_relays.relays.values():
        server.add_stream(stream)
    try:
        type_relays.start()
        server.start()
    except KeyboardInterrupt:
        pass
    finally:
        server.stop()


if __name__ == "__main__":
    main()
