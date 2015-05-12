from __future__ import unicode_literals, print_function

import shelve

import tornado.ioloop
import tornado.web
import ztreamy


class DriverDataClient(ztreamy.Client):
    database_name = 'hermes_driverdata.db'
    application_id = 'SmartDriver'

    def __init__(self, source_urls):
        super(DriverDataClient, self).__init__( \
                    source_urls,
                    ztreamy.ApplicationFilter(self._event_callback,
                             application_id=DriverDataClient.application_id),
                    connection_close_callback=self._connection_close_callback)
        self.db = shelve.open(DriverDataClient.database_name)

    def close(self):
        self.db.close()
        self.stop()

    def get(self, source_id):
        return self.db.get(source_id)

    def _event_callback(self, event):
        self.db[event.source_id] = event

    def _connection_close_callback(self):
        pass


class LatestDriverDataHandler(tornado.web.RequestHandler):
    def initialize(self, data_client):
        self.data_client = data_client

    def get(self):
        source_id = self.get_query_argument('user', default=None)
        if source_id is not None:
            event = self.data_client.get(source_id.encode('utf-8'))
            if event is not None:
                self.set_header('Content-Type', ztreamy.json_media_type)
                self.write(event.serialize_json())
            else:
                self.send_error(status_code=404)
        else:
            self.send_error(status_code=404)


def _read_cmd_options():
    from optparse import OptionParser, Values
    import tornado.options
    tornado.options.define('port', default=9101, help='run on the given port',
                           type=int)
    tornado.options.define('path', default='/last_driver_data',
                           help='Resource path beginning with a slash')
    remaining = tornado.options.parse_command_line()
    options = Values()
    if len(remaining) >= 1:
        options.stream_urls = remaining
    else:
        OptionParser().error('At least one source stream URL required')
    return options

def main():
    import tornado.options
    options = _read_cmd_options()
    driver_client = DriverDataClient(options.stream_urls)
    if tornado.options.options.path.startswith('/'):
        path = tornado.options.options.path
    else:
        path = '/' + tornado.options.options.path
    application = tornado.web.Application([
        (path, LatestDriverDataHandler, {'data_client': driver_client}),
    ])
    try:
        driver_client.start()
        application.listen(tornado.options.options.port)
        tornado.ioloop.IOLoop.instance().start()
    except KeyboardInterrupt:
        pass
    finally:
        driver_client.close()


if __name__ == "__main__":
    main()
