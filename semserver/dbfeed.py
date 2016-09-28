from __future__ import unicode_literals, print_function

import gzip
import argparse
import logging
import datetime

import tornado
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


class LogDataScheduler(object):
    def __init__(self, filename, publisher, time_generator):
        self.generator = LogDataScheduler._loop_from_file(filename)
        self.publisher = publisher
        self.time_generator = time_generator
        self.io_loop = tornado.ioloop.IOLoop.instance()

    def publish_next(self):
        self.publisher.publish(next(self.generator))
        self.schedule_next()

    def schedule_next(self):
        next_time = next(self.time_generator)
        self.io_loop.add_timeout(next_time, self.publish_next)

    @staticmethod
    def _loop_from_file(filename):
        while True:
            for event in LogDataScheduler._generate_from_file(filename):
                yield event

    @staticmethod
    def _generate_from_file(filename):
        send_from_timestamp_citizen = \
            ztreamy.parse_timestamp('2015-11-18T17:00:00+02:00')
        send_from_timestamp_driver = \
            ztreamy.parse_timestamp('2015-11-10T00:00:00+02:00')
        if filename.endswith('.gz'):
            file_ = gzip.GzipFile(filename, 'r')
        else:
            file_ = open(filename, 'r')
        deserializer = ztreamy.events.Deserializer()
        while True:
            data = file_.read(1024)
            if data == '':
                break
            evs = deserializer.deserialize(data, parse_body=False,
                                           complete=False)
            for event in evs:
                try:
                    t = ztreamy.parse_timestamp(event.timestamp)
                except ztreamy.ZtreamyException:
                    send = False
                else:
                    if event.application_id.startswith('Hermes-Citizen-'):
                        send = t > send_from_timestamp_citizen
                    elif (event.application_id.startswith('SmartDriver')
                          and event.event_type):
                        send = t > send_from_timestamp_driver
                    else:
                        send = False
                if send:
                    event.timestamp = ztreamy.get_timestamp()
                    event.event_id = ztreamy.random_id()
                    yield event
        file_.close()


def _check_file(filename):
    try:
        with open(filename, mode='r'):
            pass
    except:
        correct = False
    else:
        correct = True
    return correct

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
