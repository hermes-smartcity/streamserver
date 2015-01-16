from __future__ import unicode_literals, print_function

import tornado.options
from ztreamy import StreamServer

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
    server = StreamServer(port)
    annotator = annotate.DriverAnnotator()
    stream = annotate.AnnotatedStream( \
                            stream_name,
                            annotator,
                            buffering_time=buffering_time,
                            allow_publish=True)
    server.add_stream(stream)
    try:
        server.start()
    except KeyboardInterrupt:
        pass
    finally:
        server.stop()


if __name__ == "__main__":
    main()
