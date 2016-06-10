from __future__ import unicode_literals, print_function

import argparse
import shelve
import os
import logging
import collections

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
                    self.stats.notify_request(scores=True,
                                              num_scores=num_results,
                                              road_info=True)
                else:
                    self.write('#i{}\r\n'.format(previous))
                    self.stats.notify_request(road_info=True)
                ## logging.debug('Sent {} locations'.format(num_results))
                self.index.insert(location, user_id, score)
            else:
                ## logging.debug('Driver didn\'t move enough')
                self.locations_long.refresh(user_id)
                self.write('#*\r\n')
                self.stats.notify_request()


class ScoreIndex(locations.LocationIndex):
    def __init__(self, ioloop, ttl=600, allow_same_user=False):
        # By now, locations and scores stay 3 days in the DB.
        # In the future, about 30 min or less would be enough
#        super(ScoreIndex, self).__init__(500.0, ttl=259200)
        super(ScoreIndex, self).__init__(500.0, ttl=ttl,
                                         allow_same_user=allow_same_user)
        # Roll every ttl / 4 (seconds) = ttl * 250 (milliseconds)
        tornado.ioloop.PeriodicCallback(self.roll, ttl * 250, ioloop).start()


class LatestLocations(utils.LatestValueBuffer):
    def __init__(self, threshold_distance, ioloop):
        super(LatestLocations, self).__init__()
        self.threshold_distance = threshold_distance
        tornado.ioloop.PeriodicCallback(self.roll, 30000, ioloop).start()

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


PeriodStats = collections.namedtuple('PeriodStats',
                                     ('requests',
                                      'scores_requests',
                                      'road_info_requests',
                                      'scores',
                                      'total_time',
                                      'size_score_index',
                                      'size_locations_short',
                                      'size_locations_long'),
                                     verbose=False)


class StatsTracker(object):
    def __init__(self, score_index, locations_short, locations_long):
        self.score_index = score_index
        self.locations_short = locations_short
        self.locations_long = locations_long
        self.latest_times = os.times()
        self.num_requests = 0
        self.num_scores_requests = 0
        self.num_road_info_requests = 0
        self.num_scores = 0

    def notify_request(self, scores=False, num_scores=0, road_info=False):
        self.num_requests += 1
        if scores:
            self.num_scores_requests += 1
            self.num_scores += num_scores
        if road_info:
            self.num_road_info_requests += 1

    def compute_cycle(self):
        current_times = os.times()
        user_time = current_times[0] - self.latest_times[0]
        sys_time = current_times[1] - self.latest_times[1]
        total_time = user_time + sys_time
        stats = PeriodStats(self.num_requests,
                            self.num_scores_requests,
                            self.num_road_info_requests,
                            self.num_scores,
                            total_time,
                            len(self.score_index),
                            len(self.locations_short),
                            len(self.locations_long))
        self.num_requests = 0
        self.num_scores_requests = 0
        self.num_road_info_requests = 0
        self.num_scores = 0
        self.latest_times = current_times
        return stats

    def log_stats(self):
        stats = self.compute_cycle()
        logging.info('restserver (60s): '
                     '{} r / {:.02f}s / {} s / {} ri / {} ss'.\
                     format(stats.requests, stats.total_time,
                            stats.scores_requests, stats.road_info_requests,
                            stats.scores))
        logging.info('sizes: {} sc_idx / {} shrt_loc / {} lng_loc'.\
                     format(stats.size_score_index,
                            stats.size_locations_short,
                            stats.size_locations_long))


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


def main():
    args = _read_cmd_arguments()
    utils.configure_logging('restserver', level=args.log_level)
    ## driver_client = DriverDataClient(args.collectors)
    ## sleep_client = SleepDataClient(args.collectors)
    ## steps_client = StepsDataClient(args.collectors)
    score_index = ScoreIndex(tornado.ioloop.IOLoop.instance(),
                             ttl=args.index_ttl,
                             allow_same_user=args.allow_same_user)
    locations_short = LatestLocations(10.0, tornado.ioloop.IOLoop.instance())
    locations_long = LatestLocations(300.0, tornado.ioloop.IOLoop.instance())
    stats_tracker = StatsTracker(score_index, locations_short, locations_long)
    application = tornado.web.Application([
        ## ('/last_driver_data', LatestDataHandler,
        ##  {'data_client': driver_client}),
        ## ('/last_sleep_data', LatestDataHandler,
        ##  {'data_client': sleep_client}),
        ## ('/last_steps_data', LatestDataHandler,
        ##  {'data_client': steps_client}),
        ('/driver_scores', DriverScoresHandler,
         {'index': score_index,
          'locations_short': locations_short,
          'locations_long': locations_long,
          'stats': stats_tracker,
         }),
    ])
    try:
        ## driver_client.start()
        ## sleep_client.start()
        ## steps_client.start()
        application.listen(args.port)
        tornado.ioloop.PeriodicCallback(stats_tracker.log_stats, 60000).start()
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
