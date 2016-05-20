from __future__ import unicode_literals, print_function

import argparse
import logging
import os

import tornado.web
import tornado.gen
import tornado.httpclient
import tornado.httputil
import ztreamy
import ztreamy.server
import ztreamy.client

from . import utils
from . import collector


class BackendStream(ztreamy.Stream):
    def __init__(self, buffering_time, disable_persistence=False):
        super(BackendStream, self).__init__('backend',
                                label='semserver-backend',
                                num_recent_events=16384,
                                persist_events=not disable_persistence,
                                parse_event_body=False,
                                buffering_time=buffering_time,
                                allow_publish=True)
        self.timers = [
            tornado.ioloop.PeriodicCallback(self._periodic_stats,
                                            30000,
                                            io_loop=self.ioloop),
        ]
        self.num_events = 0
        self.latest_times = os.times()

    def start(self):
        super(BackendStream, self).start()
        for timer in self.timers:
            timer.start()

    def stop(self):
        super(BackendStream, self).stop()
        for timer in self.timers:
            timer.stop()

    def count_events(self, num_events):
        self.num_events += num_events

    def _roll_latest_locations(self):
        logging.debug('Roll latest locations buffer')
        self.latest_locations.roll()

    def _periodic_stats(self):
        logging.info('Events in the last 30s: {}'.format(self.num_events))
        self.num_events = 0
        current_times = os.times()
        user_time = current_times[0] - self.latest_times[0]
        sys_time = current_times[1] - self.latest_times[1]
        total_time = user_time + sys_time
        self.latest_times = current_times
        logging.info('Time: {} = {} + {}'.format(total_time, user_time,
                                                 sys_time))


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
    utils.configure_logging('backend', level=args.log_level)
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
