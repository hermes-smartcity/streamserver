import argparse

import ztreamy.client

from . import utils
from . import collector


def _read_cmd_arguments():
    parser = argparse.ArgumentParser( \
                    description='Run the HERMES frontend server.')
    parser.add_argument('--disable-feedback', dest='disable_feedback',
                        action='store_true')
    parser.add_argument('--disable-road-info', dest='disable_road_info',
                        action='store_true')
    parser.add_argument('-k', '--backend-stream', dest='backend_stream',
                        default='http://localhost:9109/backend/',
                        help='Backend stream URL')
    parser.add_argument('-i', '--score-info-url', dest='score_info_url',
                        default=collector.DEFAULT_SCORE_INFO_URL,
                        help='Scores info service URL')
    parser.add_argument('-r', '--road-info-url', dest='road_info_url',
                        default=collector.DEFAULT_ROAD_INFO_URL,
                        help='Road info service URL')

    utils.add_server_options(parser, 9110, stream=True)
    args = parser.parse_args()
    return args

def _create_stream_server(port, buffering_time, disable_feedback=False,
                          disable_road_info=False,
                          backend_stream=None,
                          score_info_url=collector.DEFAULT_SCORE_INFO_URL,
                          road_info_url=collector.DEFAULT_ROAD_INFO_URL):
    server = ztreamy.StreamServer(port, xheaders=True)
    stream = collector.CollectorStream(buffering_time,
                                       label='frontend-{}'.format(port),
                                       disable_feedback=disable_feedback,
                                       disable_road_info=disable_road_info,
                                       disable_persistence=True,
                                       backend_stream=backend_stream,
                                       score_info_url=score_info_url,
                                       road_info_url=road_info_url)
    server.add_stream(stream)
    return server

def main():
    args = _read_cmd_arguments()
    if args.buffer > 0:
        buffering_time = args.buffer * 1000
    else:
        buffering_time = None
    utils.configure_logging('frontend', level=args.log_level)
    server = _create_stream_server( \
                                args.port,
                                buffering_time,
                                disable_feedback=args.disable_feedback,
                                disable_road_info=args.disable_road_info,
                                backend_stream=args.backend_stream,
                                score_info_url=args.score_info_url,
                                road_info_url=args.road_info_url)
    ztreamy.client.configure_max_clients(1000)
    try:
        server.start()
    except KeyboardInterrupt:
        pass
    finally:
        server.stop()


if __name__ == "__main__":
    main()
