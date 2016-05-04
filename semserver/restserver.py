from __future__ import unicode_literals, print_function

import argparse
import shelve

import tornado.ioloop
import tornado.web
import ztreamy

from . import utils
from . import locations


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


class DriverScoresHandler(tornado.web.RequestHandler):
    def initialize(self, index):
        self.index = index

    def get(self):
        try:
            user_id = self.get_query_argument('user')
            latitude = float(self.get_query_argument('latitude'))
            longitude = float(self.get_query_argument('longitude'))
            score = float(self.get_query_argument('score'))
        except (tornado.web.MissingArgumentError, ValueError):
            self.send_error(status_code=422)
        else:
            location = locations.Location(latitude, longitude)
            self.set_header('Content-Type', 'text/plain')
            for o_location, o_score in self.index.lookup(location, user_id):
                self.write('{},{},{}\r\n'.format(o_location.lat,
                                                 o_location.long,
                                                 o_score))
            self.index.insert(location, user_id, score)


class ScoreIndex(locations.LocationIndex):
    def __init__(self, ioloop):
        # By now, locations and scores stay 3 days in the DB.
        # In the future, about 30 min or less would be enough
        super(ScoreIndex, self).__init__(500.0, ttl=259200)
        tornado.ioloop.PeriodicCallback(self.roll, 86400000, ioloop)


def _read_cmd_arguments():
    parser = argparse.ArgumentParser( \
                    description='Run the HERMES REST server.')
    parser.add_argument('-p', '--port', type=int, dest='port',
                        default=9101, help='TCP port to use')
    parser.add_argument('collectors', nargs='*',
                        default=['http://localhost:9100/collector/compressed'],
                        help='collector stream URLs')
    args = parser.parse_args()
    return args

def main():
    args = _read_cmd_arguments()
    utils.configure_logging('restserver')
    driver_client = DriverDataClient(args.collectors)
    sleep_client = SleepDataClient(args.collectors)
    steps_client = StepsDataClient(args.collectors)
    application = tornado.web.Application([
        ('/last_driver_data', LatestDataHandler,
         {'data_client': driver_client}),
        ('/last_sleep_data', LatestDataHandler,
         {'data_client': sleep_client}),
        ('/last_steps_data', LatestDataHandler,
         {'data_client': steps_client}),
        ('/driver_scores', DriverScoresHandler,
         {'index': ScoreIndex(tornado.ioloop.IOLoop.instance())}),
    ])
    try:
        driver_client.start()
        sleep_client.start()
        steps_client.start()
        application.listen(args.port)
        tornado.ioloop.IOLoop.instance().start()
    except KeyboardInterrupt:
        pass
    finally:
        driver_client.close()
        sleep_client.close()
        steps_client.close()


if __name__ == "__main__":
    main()
