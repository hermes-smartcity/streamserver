from __future__ import unicode_literals, print_function

import tornado.options
import ztreamy

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


def main():
    tornado.options.define('port', default=9100, help='run on the given port',
                           type=int)
    tornado.options.define('buffer', default=2.0, help='event buffer time (s)',
                           type=float)
    tornado.options.define('preload', default=None,
                           help='preload events from file')
    tornado.options.parse_command_line()
    port = tornado.options.options.port
    preload_file = tornado.options.options.preload
    if (tornado.options.options.buffer is not None
        and tornado.options.options.buffer > 0):
        buffering_time = tornado.options.options.buffer * 1000
    else:
        buffering_time = None
    server = ztreamy.StreamServer(port)
    collector_stream = ztreamy.Stream('collector',
                                      parse_event_body=True,
                                      buffering_time=buffering_time,
                                      allow_publish=True)
    if preload_file:
        with open(preload_file, 'rb') as f:
            collector_stream.preload_recent_events_buffer_from_file(f)
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
