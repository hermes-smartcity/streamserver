from __future__ import unicode_literals, print_function

import gzip
import argparse

import tornado
import ztreamy
import ztreamy.client
import ztreamy.tools.utils

from . import utils


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
    parser.add_argument('-d', '--distribution', dest='distribution',
                    default='exp[0.1]',
                    help='event statistical distribution of the test stream')
    parser.add_argument('collectors', nargs='*',
                        default=['http://localhost:9100/collector/compressed'],
                        help='collector stream URLs')
    args = parser.parse_args()
    return args

def main():
    args = _read_cmd_arguments()
    if args.buffer > 0:
        buffering_time = args.buffer * 1000
    else:
        buffering_time = None
    utils.configure_logging('dbfeed', level=args.log_level)
    server = ztreamy.StreamServer(args.port)
    stream = ztreamy.RelayStream('dbfeed',
                                 args.collectors,
                                 label='semserver-dbfeed',
                                 num_recent_events=16384,
                                 persist_events=True,
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
