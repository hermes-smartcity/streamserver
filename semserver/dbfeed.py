from __future__ import unicode_literals, print_function

import gzip

import tornado
import ztreamy
import ztreamy.client
import ztreamy.tools.utils


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
        send_from_timestamp = \
            ztreamy.rfc3339_as_time('2015-09-01T00:00:00+02:00')
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
                    t = ztreamy.rfc3339_as_time(event.timestamp)
                except ztreamy.ZtreamyException:
                    send = False
                else:
                    send = t > send_from_timestamp
                if send:
                    event.timestamp = ztreamy.get_timestamp()
                    event.event_id = ztreamy.random_id()
                    yield event
        file_.close()


def _read_cmd_options():
    from optparse import Values
    import tornado.options
    tornado.options.define('port',
                           default=9102,
                           help='run on the given port',
                           type=int)
    tornado.options.define('buffer',
                           default=2.0,
                           help='event buffer time (s)',
                           type=float)
    tornado.options.define('distribution',
                    default='exp[0.1]',
                    help='event statistical distribution of the test stream')
    remaining = tornado.options.parse_command_line()
    options = Values()
    if len(remaining) >= 1:
        options.stream_urls = remaining
    else:
        options.stream_urls = ['http://localhost:9100/collector/compressed']
    return options

def main():
    import tornado.options
    options = _read_cmd_options()
    buffering_time = tornado.options.options.buffer * 1000
    port = tornado.options.options.port
    src_stream_urls = options.stream_urls
    server = ztreamy.StreamServer(port)
    stream = ztreamy.RelayStream('dbfeed',
                                 src_stream_urls,
                                 buffering_time=buffering_time)
    test_stream = ztreamy.RelayStream('dbfeed-test',
                                 src_stream_urls,
                                 buffering_time=buffering_time)
    server.add_stream(stream)
    server.add_stream(test_stream)
    debug_publisher = ztreamy.client.LocalEventPublisher(test_stream)
    scheduler = ztreamy.tools.utils.get_scheduler( \
                                    tornado.options.options.distribution)
    log_data_scheduler = LogDataScheduler('log-hermes.txt',
                                          debug_publisher,
                                          scheduler)
    log_data_scheduler.schedule_next()
    try:
        server.start()
    except KeyboardInterrupt:
        pass
    finally:
        server.stop()


if __name__ == "__main__":
    main()
