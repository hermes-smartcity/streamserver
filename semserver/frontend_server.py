import argparse

import ztreamy.client

from . import utils
from . import collector


def _create_stream_server(port, buffering_time, disable_feedback=False,
                          disable_road_info=False,
                          backend_stream=None,
                          score_info_url=collector.DEFAULT_SCORE_INFO_URL,
                          road_info_url=collector.DEFAULT_ROAD_INFO_URL,
                          log_event_time=None):
    server = ztreamy.StreamServer(port, xheaders=True)
    stream = collector.CollectorStream(buffering_time,
                                       label='frontend-{}'.format(port),
                                       disable_feedback=disable_feedback,
                                       disable_road_info=disable_road_info,
                                       disable_persistence=True,
                                       backend_stream=backend_stream,
                                       score_info_url=score_info_url,
                                       road_info_url=road_info_url,
                                       log_event_time=log_event_time)
    server.add_stream(stream)
    return server

def main():
    args = collector.read_cmd_arguments(default_port=9110)
    if args.buffer > 0:
        buffering_time = args.buffer * 1000
    else:
        buffering_time = None
    utils.configure_logging('frontend-{}'.format(args.port),
                            level=args.log_level,
                            disable_stderr=args.disable_stderr)
    server = _create_stream_server( \
                                args.port,
                                buffering_time,
                                disable_feedback=args.disable_feedback,
                                disable_road_info=args.disable_road_info,
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
