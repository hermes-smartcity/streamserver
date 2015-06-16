from __future__ import unicode_literals, print_function

from ztreamy import StreamServer

from . import annotate


def main():
    buffering_time = 1000
    src_stream_uri = 'http://localhost:9100/collector/priority'
    server = StreamServer(9101)
    annotator = annotate.HermesAnnotator()
    stream = annotate.AnnotatedRelayStream( \
                            'annotated',
                            [src_stream_uri],
                            annotator,
                            buffering_time=buffering_time)
    server.add_stream(stream)
    try:
        server.start()
    except KeyboardInterrupt:
        pass
    finally:
        server.stop()


if __name__ == "__main__":
    main()
