from __future__ import unicode_literals, print_function

import argparse
import shelve
import os
import logging

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
    def initialize(self, index, locations_short, locations_long, stats):
        self.index = index
        self.locations_short = locations_short
        self.locations_long = locations_long
        self.stats = stats

    def get(self):
        self.stats.notify_request()
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
            check, previous = self.locations_short.check(user_id, location)
            if check:
                # Check the long locations buffer to decide whether
                # to get the scores of other drivers
                check, previous = self.locations_long.check(user_id, location)
                if check:
                    self.write('#+{}\r\n'.format(previous))
                    num_results = 0
                    for o_loc, o_score in self.index.lookup(location, user_id):
                        self.write('{},{},{}\r\n'.format(o_loc.lat,
                                                         o_loc.long,
                                                         o_score))
                        num_results += 1
                        if num_results == 10:
                            break
                else:
                    self.write('#i{}\r\n'.format(previous))
                ## logging.debug('Sent {} locations'.format(num_results))
                self.index.insert(location, user_id, score)
            else:
                ## logging.debug('Driver didn\'t move enough')
                self.locations_long.refresh(user_id)
                self.write('#*\r\n')


class ScoreIndex(locations.LocationIndex):
    def __init__(self, ioloop, ttl=600, allow_same_user=False):
        # By now, locations and scores stay 3 days in the DB.
        # In the future, about 30 min or less would be enough
#        super(ScoreIndex, self).__init__(500.0, ttl=259200)
        super(ScoreIndex, self).__init__(500.0, ttl=ttl,
                                         allow_same_user=allow_same_user)
        tornado.ioloop.PeriodicCallback(self.roll, ttl / 4, ioloop)


class LatestLocations(utils.LatestValueBuffer):
    def __init__(self, threshold_distance, ioloop):
        super(LatestLocations, self).__init__()
        self.threshold_distance = threshold_distance
        tornado.ioloop.PeriodicCallback(self.roll, 60000, ioloop)

    def check(self, user_id, location):
        try:
            previous = self[user_id]
        except KeyError:
            answer = True
            previous = location
        else:
            if location.distance(previous) >= self.threshold_distance:
                answer = True
            else:
                answer = False
        if answer:
            self[user_id] = location
        else:
            self.refresh(user_id)
        return answer, previous


def _read_cmd_arguments():
    parser = argparse.ArgumentParser( \
                    description='Run the HERMES REST server.')
    ## parser.add_argument('collectors', nargs='*',
    ##                  default=['http://localhost:9100/collector/compressed'],
    ##                  help='collector stream URLs')
    parser.add_argument('-t', '--index-ttl', type=float, dest='index_ttl',
                        default=600.0,
                        help=('Time to live for entries in the location '
                              'index'))
    parser.add_argument('-s', '--allow-same-user', dest='allow_same_user',
                        action='store_true',
                        help=('List the score of the same user also '
                              '(for testing purposes only)'))
    utils.add_server_options(parser, 9101)
    args = parser.parse_args()
    return args


class StatsTimer(object):
    def __init__(self, period):
        self.latest_times = os.times()
        self.num_requests = 0
        tornado.ioloop.PeriodicCallback(self._periodic_stats, period * 1000)\
            .start()

    def notify_request(self):
        self.num_requests += 1

    def _periodic_stats(self):
        logging.info('Requests in the last 30s: {}'.format(self.num_requests))
        self.num_requests = 0
        current_times = os.times()
        user_time = current_times[0] - self.latest_times[0]
        sys_time = current_times[1] - self.latest_times[1]
        total_time = user_time + sys_time
        self.latest_times = current_times
        logging.info('Time: {} = {} + {}'.format(total_time, user_time,
                                                 sys_time))


def main():
    args = _read_cmd_arguments()
    utils.configure_logging('restserver', level=args.log_level)
    ## driver_client = DriverDataClient(args.collectors)
    ## sleep_client = SleepDataClient(args.collectors)
    ## steps_client = StepsDataClient(args.collectors)
    application = tornado.web.Application([
        ## ('/last_driver_data', LatestDataHandler,
        ##  {'data_client': driver_client}),
        ## ('/last_sleep_data', LatestDataHandler,
        ##  {'data_client': sleep_client}),
        ## ('/last_steps_data', LatestDataHandler,
        ##  {'data_client': steps_client}),
        ('/driver_scores', DriverScoresHandler,
         {'index': ScoreIndex(tornado.ioloop.IOLoop.instance(),
                              ttl=args.index_ttl,
                              allow_same_user=args.allow_same_user),
          'locations_short': LatestLocations(10.0,
                                         tornado.ioloop.IOLoop.instance()),
          'locations_long': LatestLocations(300.0,
                                         tornado.ioloop.IOLoop.instance()),
          'stats': StatsTimer(30.0),
         }),
    ])
    try:
        ## driver_client.start()
        ## sleep_client.start()
        ## steps_client.start()
        application.listen(args.port)

        tornado.ioloop.IOLoop.instance().start()
    except KeyboardInterrupt:
        pass
    finally:
        pass
        ## driver_client.close()
        ## sleep_client.close()
        ## steps_client.close()


if __name__ == "__main__":
    main()
