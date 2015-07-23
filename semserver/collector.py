from __future__ import unicode_literals, print_function

import tornado.options
import ztreamy


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
    server.add_stream(collector_stream)
    try:
        server.start()
    except KeyboardInterrupt:
        pass
    finally:
        server.stop()


if __name__ == "__main__":
    main()
