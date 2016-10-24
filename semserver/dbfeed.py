from __future__ import unicode_literals, print_function

import argparse
import datetime

import ztreamy
import ztreamy.client
import ztreamy.tools.utils

from . import utils


class DBFeedStream(ztreamy.RelayStream):
    def __init__(self, *args, **kwargs):
        super(DBFeedStream, self).__init__(*args, filter_=DBFeedFilter(),
                                           **kwargs)
        self.timers = [
            ## tornado.ioloop.PeriodicCallback(self._periodic_stats,
            ##                                 60000,
            ##                                 io_loop=self.ioloop),
        ]
        self.stats_tracker = utils.StatsTracker(self)
        self._schedule_next_stats_period()

    def start(self):
        super(DBFeedStream, self).start()
        for timer in self.timers:
            timer.start()

    def stop(self):
        super(DBFeedStream, self).stop()
        for timer in self.timers:
            timer.stop()

    def _periodic_stats(self):
        stats = self.stats_tracker.compute_cycle()
        utils.log_stats_value(self.label, stats)
        self._schedule_next_stats_period()

    def _schedule_next_stats_period(self):
        self.ioloop.add_timeout(datetime.timedelta( \
                                        seconds=60 - self.ioloop.time() % 60),
                                self._periodic_stats)


class DBFeedFilter(ztreamy.Filter):
    def __init__(self):
        """Creates the default db filter.

        """
        super(DBFeedFilter, self).__init__(None)

    def filter_event(self, event):
        if (event.application_id != 'SmartDriver'
            or event.event_type != 'Vehicle Location'):
            self.callback(event)


def _read_cmd_arguments():
    parser = argparse.ArgumentParser( \
                    description='Run the HERMES dbfeed server.')
    utils.add_server_options(parser, 9102, stream=True)
    parser.add_argument('--disable-persistence', dest='disable_persistence',
                        action='store_true')
    parser.add_argument('collectors', nargs='*',
                        default=['http://localhost:9109/backend/compressed'],
                        help='collector/backend stream URLs')
    args = parser.parse_args()
    return args

def main():
    args = _read_cmd_arguments()
    if args.buffer > 0:
        buffering_time = args.buffer * 1000
    else:
        buffering_time = None
    utils.configure_logging('dbfeed-{}'.format(args.port),
                            level=args.log_level,
                            disable_stderr=args.disable_stderr)
    server = ztreamy.StreamServer(args.port)
    stream = DBFeedStream('dbfeed',
                          args.collectors,
                          label='dbfeed',
                          num_recent_events=2**17,
                          persist_events=not args.disable_persistence,
                          buffering_time=buffering_time,
                          retrieve_missing_events=True)
    server.add_stream(stream)
    try:
        server.start()
    except KeyboardInterrupt:
        pass
    finally:
        server.stop()


if __name__ == "__main__":
    main()
