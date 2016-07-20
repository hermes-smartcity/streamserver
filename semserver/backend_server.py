from __future__ import unicode_literals, print_function

import argparse
import logging
import datetime

import ztreamy
import ztreamy.server
import ztreamy.client

from . import utils
from . import collector


class BackendStream(ztreamy.Stream):
    def __init__(self, buffering_time, disable_persistence=False):
        super(BackendStream, self).__init__('backend',
                                label='backend',
                                num_recent_events=2**16,
                                persist_events=not disable_persistence,
                                parse_event_body=False,
                                buffering_time=buffering_time,
                                allow_publish=True)
        self.timers = [
            ## tornado.ioloop.PeriodicCallback(self._periodic_stats,
            ##                                 60000,
            ##                                 io_loop=self.ioloop),
        ]
        self.stats_tracker = utils.StatsTracker(self)
        self._schedule_next_stats_period()


    def start(self):
        super(BackendStream, self).start()
        for timer in self.timers:
            timer.start()

    def stop(self):
        super(BackendStream, self).stop()
        for timer in self.timers:
            timer.stop()

    def _roll_latest_locations(self):
        logging.debug('Roll latest locations buffer')
        self.latest_locations.roll()

    def _periodic_stats(self):
        stats = self.stats_tracker.compute_cycle()
        logging.info(('{0} (60s): {1.num_events} ev / {1.cpu_time:.02f}s '
                      ' / u: {1.utilization:.03f}')\
                     .format(self.label, stats))
        self._schedule_next_stats_period()

    def _schedule_next_stats_period(self):
        self.ioloop.add_timeout(datetime.timedelta( \
                                        seconds=60 - self.ioloop.time() % 60),
                                self._periodic_stats)


def _read_cmd_arguments():
    parser = argparse.ArgumentParser( \
                    description='Run the HERMES backend stream server.')
    parser.add_argument('--disable-persistence', dest='disable_persistence',
                        action='store_true')
    utils.add_server_options(parser, 9109, stream=True)
    args = parser.parse_args()
    return args


def _create_stream_server(port, buffering_time, disable_persistence=False):
    server = ztreamy.StreamServer(port)
    backend_stream = BackendStream(buffering_time,
                                   disable_persistence=disable_persistence)
    type_relays = collector.EventTypeRelays(backend_stream,
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
    server.add_stream(backend_stream)
    for stream in type_relays.relays.values():
        server.add_stream(stream)
    return server

def main():
    args = _read_cmd_arguments()
    if args.buffer > 0:
        buffering_time = args.buffer * 1000
    else:
        buffering_time = None
    utils.configure_logging('backend', level=args.log_level,
                            disable_stderr=args.disable_stderr)
    server = _create_stream_server( \
                                args.port,
                                buffering_time,
                                disable_persistence=args.disable_persistence)
    try:
        server.start()
    except KeyboardInterrupt:
        pass
    finally:
        server.stop()


if __name__ == "__main__":
    main()
