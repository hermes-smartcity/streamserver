from __future__ import unicode_literals, print_function

import argparse

import ztreamy

from . import utils


def _read_cmd_arguments():
    parser = argparse.ArgumentParser( \
                    description='Run the HERMES test server.')
    utils.add_server_options(parser, 9105, stream=True)
    args = parser.parse_args()
    return args

def main():
    args = _read_cmd_arguments()
    if args.buffer > 0:
        buffering_time = args.buffer * 1000
    else:
        buffering_time = None
    utils.configure_logging('teststream', level=args.log_level)
    server = ztreamy.StreamServer(args.port)
    test_stream = ztreamy.Stream('test',
                                      parse_event_body=True,
                                      buffering_time=buffering_time,
                                      allow_publish=True)
    server.add_stream(test_stream)
    try:
        server.start()
    except KeyboardInterrupt:
        pass
    finally:
        server.stop()


if __name__ == "__main__":
    main()
