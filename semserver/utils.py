import logging
import logging.handlers
import os
import os.path
import cStringIO
import gzip
import json
import collections
import itertools


DIRNAME_LOGGING = 'logs-semserver'


class LatestValueBuffer(collections.MutableMapping):
    """Stores the latest data value associated to a key.

    Older data items can be periodically removed through the `roll` method.

    """
    def __init__(self):
        self.current = {}
        self.previous = {}

    def roll(self):
        self.previous = self.current
        self.current = {}

    def refresh(self, key):
        if not key in self.current:
            self.current[key] = self.previous[key]

    def __contains__(self, key):
        return key in self.current or key in self.previous

    def __setitem__(self, key, value):
        self.current[key] = value

    def __getitem__(self, key):
        try:
            result = self.current[key]
        except KeyError:
            result = self.previous[key]
        return result

    def __delitem__(self, key):
        try:
            del self.current[key]
        except KeyError:
            del self.previous[key]

    def __len__(self):
        # Note the value is incorrect because of intersections,
        # but we don't want to worry about this matter.
        return len(self.current) + len(self.previous)

    def __iter__(self):
        return itertools.chain(self.current, self.previous)


class StatsTracker(object):
    """Periodically computes CPU time and number of events of a stream."""
    def __init__(self, stream):
        self.stream = stream
        self.last_num_events = self.stream.stats.num_events
        self.last_times = os.times()

    def compute_cycle(self):
        # Number of published events
        num_events = self.stream.stats.num_events - self.last_num_events
        self.last_num_events = self.stream.stats.num_events
        # CPU time
        current_times = os.times()
        stats = StatsValue(num_events, current_times, self.last_times)
        self.last_times = current_times
        return stats


class StatsValue(object):
    """Object returned by the StatsTracker class."""
    def __init__(self, num_events, current_times, last_times):
        self.num_events = num_events
        # current_times, last_times: 0 is user_time; 1 is sys_time
        self.cpu_time = (current_times[0] - last_times[0]
                         + current_times[1] - last_times[1])
        self.real_time = current_times[4] - last_times[4]

    @property
    def utilization(self):
        return self.cpu_time / self.real_time

    @property
    def events_per_second(self):
        return self.num_events / self.real_time

    @property
    def time_per_event(self):
        return self.cpu_time / self.num_events


def configure_logging(module_name, level='info', dirname=DIRNAME_LOGGING):
    if not os.path.exists(dirname):
        os.makedirs(dirname)
    filename = os.path.join(dirname, module_name + '.log')
    log_format = '%(asctime)-15s %(levelname)s %(message)s'
    date_format = '%Y%m%d %H:%M:%S'
    level = _log_level(level)
    logging.basicConfig(level=level,
                        format=log_format,
                        datefmt=date_format)
    # define a Handler which writes INFO messages or higher to the sys.stderr
    file_handler = logging.handlers.WatchedFileHandler(filename)
    file_handler.setFormatter(logging.Formatter(fmt=log_format,
                                                datefmt=date_format))
    file_handler.setLevel(level)
    logging.getLogger('').addHandler(file_handler)
    return filename

def add_server_options(parser, default_port, stream=False):
    parser.add_argument('-p', '--port', type=int, dest='port',
                        default=default_port, help='TCP port to use')
    parser.add_argument('--log-level', dest='log_level',
                        choices=['warn', 'info', 'debug'],
                        default='info')
    if stream:
        parser.add_argument('-b', '--buffer', type=float, dest='buffer',
                            default=2.0,
                            help='Buffer time in seconds (0 for no buffering)')



def serialize_object_json(data, compress=False):
    serialized = json.dumps(data.as_dict())
    if compress:
        serialized = compress_gzip(serialized)
    return serialized

def compress_gzip(data):
    output = cStringIO.StringIO()
    with gzip.GzipFile(fileobj=output, mode='wb') as gzip_file:
        gzip_file.write(data)
    compressed_data = output.getvalue()
    output.close()
    return compressed_data

def _log_level(log_level):
    if log_level == 'warn':
        return logging.WARN
    elif log_level == 'info':
        return logging.INFO
    elif log_level == 'debug':
        return logging.DEBUG
    elif log_level in (logging.WARN, logging.INFO, logging.DEBUG):
        return log_level
    else:
        raise ValueError('Invalid log level string')
