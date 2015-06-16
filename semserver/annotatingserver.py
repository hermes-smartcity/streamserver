from __future__ import unicode_literals, print_function

import tornado.options
import ztreamy

from . import annotate


def main():
    tornado.options.define('port', default=9100, help='run on the given port',
                           type=int)
    tornado.options.define('name', default='events',
                           help='stream name')
    tornado.options.define('buffer', default=None, help='event buffer time (s)',
                           type=float)
    tornado.options.parse_command_line()
    port = tornado.options.options.port
    stream_name = tornado.options.options.name
    if (tornado.options.options.buffer is not None
        and tornado.options.options.buffer > 0):
        buffering_time = tornado.options.options.buffer * 1000
    else:
        buffering_time = None
    server = ztreamy.StreamServer(port)
    collector_stream = ztreamy.Stream(stream_name,
                                      parse_event_body=True,
                                      buffering_time=buffering_time,
                                      allow_publish=True)
    annotator = annotate.HermesAnnotator()
    annotated_stream = annotate.AnnotatedRelayStream( \
                                        'annotated',
                                        collector_stream,
                                        annotator,
                                        buffering_time=buffering_time)
    server.add_stream(collector_stream)
    server.add_stream(annotated_stream)
    try:
        server.start()
    except KeyboardInterrupt:
        pass
    finally:
        server.stop()


if __name__ == "__main__":
    main()
