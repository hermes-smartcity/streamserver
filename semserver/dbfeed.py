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
            ztreamy.rfc3339_as_time('2015-05-29T00:00:00+02:00')
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



def main():
    buffering_time = 1000
    src_stream_uri = 'http://localhost:9100/collector/compressed'
    server = ztreamy.StreamServer(9102)
    stream = ztreamy.RelayStream('dbfeed',
                                 [src_stream_uri],
                                 buffering_time=buffering_time)
    server.add_stream(stream)
    debug_publisher = ztreamy.client.LocalEventPublisher(stream)
    log_data_scheduler = LogDataScheduler('log-hermes-sample-4.txt.gz',
                                debug_publisher,
                                ztreamy.tools.utils.get_scheduler('exp[5.0]'))
    log_data_scheduler.schedule_next()
    try:
        server.start()
    except KeyboardInterrupt:
        pass
    finally:
        server.stop()


if __name__ == "__main__":
    main()
