from __future__ import unicode_literals, print_function

import shelve

import tornado.ioloop
import tornado.web
import ztreamy


class DataClient(ztreamy.Client):
    def __init__(self, source_urls, database_name, data_filter):
        super(DataClient, self).__init__( \
                    source_urls,
                    data_filter,
                    connection_close_callback=self.connection_close_callback)
        self.db = shelve.open(database_name)

    def close(self):
        self.db.close()
        self.stop()

    def get(self, source_id):
        return self.db.get(source_id)

    def event_callback(self, event):
        self.db[event.source_id] = event

    def connection_close_callback(self):
        pass


class DriverDataClient(DataClient):
    database_name = 'hermes_driverdata.db'
    application_id = 'SmartDriver'

    def __init__(self, source_urls):
        super(DriverDataClient, self).__init__( \
                    source_urls,
                    DriverDataClient.database_name,
                    ztreamy.ApplicationFilter(self.event_callback,
                             application_id=DriverDataClient.application_id))


class SleepDataClient(DataClient):
    database_name = 'hermes_sleepdata.db'
    application_id = 'Hermes-Citizen-Fitbit-Sleep'

    def __init__(self, source_urls):
        super(SleepDataClient, self).__init__( \
                    source_urls,
                    SleepDataClient.database_name,
                    ztreamy.ApplicationFilter(self.event_callback,
                             application_id=SleepDataClient.application_id))


class StepsDataClient(DataClient):
    database_name = 'hermes_stepsdata.db'
    application_id = 'Hermes-Citizen-Fitbit-Steps'

    def __init__(self, source_urls):
        super(StepsDataClient, self).__init__( \
                    source_urls,
                    StepsDataClient.database_name,
                    ztreamy.ApplicationFilter(self.event_callback,
                             application_id=StepsDataClient.application_id))


class LatestDataHandler(tornado.web.RequestHandler):
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
    from optparse import Values
    import tornado.options
    tornado.options.define('port', default=9101, help='run on the given port',
                           type=int)
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
    driver_client = DriverDataClient(options.stream_urls)
    sleep_client = SleepDataClient(options.stream_urls)
    steps_client = StepsDataClient(options.stream_urls)
    application = tornado.web.Application([
        ('/last_driver_data', LatestDataHandler,
         {'data_client': driver_client}),
        ('/last_sleep_data', LatestDataHandler,
         {'data_client': sleep_client}),
        ('/last_steps_data', LatestDataHandler,
         {'data_client': steps_client}),
    ])
    try:
        driver_client.start()
        sleep_client.start()
        steps_client.start()
        application.listen(tornado.options.options.port)
        tornado.ioloop.IOLoop.instance().start()
    except KeyboardInterrupt:
        pass
    finally:
        driver_client.close()
        sleep_client.close()
        steps_client.close()


if __name__ == "__main__":
    main()
