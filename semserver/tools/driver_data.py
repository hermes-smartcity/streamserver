from __future__ import unicode_literals

import sys
import gzip
import collections
import argparse
import copy
import logging

import dateutil
import dateutil.parser

import ztreamy


simple_event_types = (
    'High Speed',
    'High Acceleration',
    'High Deceleration',
    'High Heart Rate',
)

ignore_source_ids = (
    '4643c6fefd6fa67bcfeed601c8dec714d08199b4f92d0fe584962480639f2161',
    'd23389efce367c6639b9e17bcf6e28477e081964987cc6de72d1fd48e186d44a',
)

_tz_madrid = dateutil.tz.tz.gettz('Europe/Madrid')
_tz_london = dateutil.tz.tz.gettz('Europe/London')


class Record(object):
    def __init__(self, user_id, default_tz=None):
        self.user_id = user_id
        self.record_type = None
        self.timestamp_ini = None
        self.latitude = None
        self.latitude_ini = None
        self.longitude = None
        self.longitude_ini = None
        self.speed = None
        self.accuracy = None
        self.accuracy_ini = None
        self.value = None
        self.mean_value = None
        self.median_value = None
        self.std_dev = None
        self.min_value = None
        self.max_value = None
        self.rr_value = None
        self._timestamp = None
        self._time = None
        if default_tz is not None:
            self.default_tz = default_tz
        else:
            self.default_tz = dateutil.tz.tz.tzoffset('', 3600)

    @property
    def timestamp(self):
        return self._timestamp

    @timestamp.setter
    def timestamp(self, value):
        self._timestamp = value
        self._time = None

    @property
    def time(self):
        if self._time is None:
            self._time = ztreamy.rfc3339_as_time(self.timestamp,
                                                 default_tz=self.default_tz)
        return self._time

    def as_row(self):
        self.check()
        return self._as_row()

    def _as_row(self):
        return [str(v) if v is not None else '' \
                for v in (self.time,
                          self.record_type,
                          self.user_id,
                          self.timestamp,
                          self.latitude,
                          self.longitude,
                          self.accuracy,
                          self.speed,
                          self.timestamp_ini,
                          self.latitude_ini,
                          self.longitude_ini,
                          self.accuracy_ini,
                          self.value,
                          self.mean_value,
                          self.median_value,
                          self.std_dev,
                          self.min_value,
                          self.max_value,
                          self.rr_value,
                          )]

    def check(self):
        if self.record_type is None:
            raise ValueError('Record without record type')
        elif self.timestamp is None:
            raise ValueError('Record without timestamp')
        elif self.latitude is None or self.longitude is None:
            raise ValueError('Record without coordinates')
        elif (self.record_type != 'Vehicle Location'
              and self.value is None and self.mean_value is None):
            raise ValueError('Record without a value')

    def clone(self):
        return copy.copy(self)


def tz_for_latitude(latitude):
    # Trick for some timestamps that came without timezone
    if latitude < 47:
        return _tz_madrid
    else:
        return _tz_london

def read_file(filename):
    with gzip.GzipFile(filename, 'r') as f:
        for events in ztreamy.Deserializer().deserialize_file(f):
            for e in events:
                if not e.source_id in ignore_source_ids:
                    yield e

def group(events):
    groups = collections.defaultdict(list)
    for event in events:
        groups[event.body.keys()[0]].append(event)
    return groups

def extract_data(event):
    try:
        if event.event_type == 'Data Section':
            records = extract_data_section_event(event)
        else:
            records = extract_simple_event(event)
    except ValueError:
        print('Warning: Value Error {}'.format(sys.exc_info()[1]))
        records = []
    return records

def extract_positions(event):
    records = []
    if 'roadSection' in event.body['Data Section']:
        samples = event.body['Data Section']['roadSection']
        for sample in samples:
            default_tz = tz_for_latitude(sample['latitude'])
            record = Record(event.source_id, default_tz=default_tz)
            record.record_type = 'Vehicle Location'
            record.latitude = sample['latitude']
            record.longitude = sample['longitude']
            record.timestamp = sample['timeStamp']
            if 'accuracy' in sample:
                record.accuracy = sample['accuracy']
            if 'speed' in sample:
                record.speed = sample['speed']
            try:
                # Check that the timestamp is correct
                record.time
            except ztreamy.ZtreamyException:
                logging.warning('Discard record because of timestamp: {}'\
                                .format(record.timestamp))
            else:
                records.append(record)
        if 'rrSection' in event.body['Data Section']:
            samples = event.body['Data Section']['rrSection']
            if len(samples) == len(records):
                for rr_value, record in zip(samples, records):
                    record.rr_value = rr_value
            elif samples:
                logging.warning('roadSection of size {} '
                                'with rrSection of size {}'.\
                                format(len(records), len(samples)))
    return records

def extract_simple_event(event):
    event_type = event.event_type
    if (event_type is not None and event_type in simple_event_types
        and event_type in event.body):
        data = event.body[event_type]
        default_tz = tz_for_latitude(data['latitude'])
        record = Record(event.source_id, default_tz=default_tz)
        record.record_type = event_type
        record.timestamp = event.timestamp
        record.value = data['value']
        record.latitude = data['latitude']
        record.longitude = data['longitude']
        if 'accuracy' in data:
            record.accuracy = data['accuracy']
        if 'speed' in data:
            record.speed = data['speed']
        elif event_type == 'High Speed':
            record.speed = data['value']
        try:
            # Check that the timestamp is correct
            record.time
        except ztreamy.ZtreamyException:
            records = []
            logging.warning('Discard record because of timestamp: {}'\
                            .format(record.timestamp))
        else:
            records = [record]
    else:
        records = []
    return records

def extract_data_section_event(event):
    records = []
    if 'Data Section' in event.body:
        data = event.body['Data Section']
        locations = extract_positions(event)
        if len(locations) >= 2:
            # Create a common base record and clone it
            base_record = Record(event.source_id)
            base_record.timestamp_ini = locations[0].timestamp
            base_record.timestamp = locations[-1].timestamp
            base_record.latitude_ini = locations[0].latitude
            base_record.latitude = locations[-1].latitude
            base_record.longitude_ini = locations[0].longitude
            base_record.longitude = locations[-1].longitude
            if hasattr(locations[0], 'accuracy'):
                base_record.accuracy_ini = locations[0].accuracy
            if hasattr(locations[-1], 'accuracy'):
                base_record.accuracy = locations[-1].accuracy
            # Vehicle speed:
            speed_record = base_record.clone()
            speed_record.record_type = 'Vehicle Speed'
            speed_record.median_value = data['medianSpeed']
            speed_record.mean_value = data['averageSpeed']
            speed_record.std_dev = data['standardDeviationSpeed']
            speed_record.min_value = data['minSpeed']
            speed_record.max_value = data['maxSpeed']
            # Driver's heart rate
            heart_record = base_record.clone()
            heart_record.record_type = 'Heart Rate'
            heart_record.mean_value = data['averageHeartRate']
            heart_record.std_dev = data['standardDeviationHeartRate']
            # Driver's RR
            rr_record = base_record.clone()
            rr_record.record_type = 'RR'
            rr_record.mean_value = data['averageRR']
            rr_record.std_dev = data['standardDeviationRR']
            # Vehicle positive kinetic energy
            pke_record = base_record.clone()
            pke_record.record_type = 'Pke'
            pke_record.value = data['pke']
            # Collect all the records into a list
            records.extend(locations)
            records.append(speed_record)
            records.append(pke_record)
            if heart_record.mean_value > 0:
                records.append(heart_record)
            if rr_record.mean_value > 0:
                records.append(rr_record)
    return records

def write_records(records):
    logging.info('Writing {} records'.format(len(records)))
    with open('data.csv', mode='a') as f:
        for record in records:
            f.write(','.join(record.as_row()))
            f.write('\n')

def clear_records_file():
    with open('data.csv', mode='w'):
        pass

def process_file(events_file, from_timestamp=None):
    clear_records_file()
    if from_timestamp is not None:
        from_time = ztreamy.rfc3339_as_time(from_timestamp)
    else:
        from_time = 0
    records = []
    num_events = 0
    num_records = 0
    for event in read_file(events_file):
        num_events += 1
        new_records = extract_data(event)
        new_records = [r for r in extract_data(event) \
                       if r.time >= from_time]
        records.extend(new_records)
        if num_events % 10000 == 0:
            logging.info('Read {} events into {} records'\
                         .format(num_events, len(records) + num_records))
            if len(records) > 3000000:
                logging.info('Sorting {} records'.format(len(records)))
                records.sort(key=lambda x: x.time)
                write_records(records[:1500000])
                del records[:1500000]
                num_records += 1500000
    logging.info('Read {} events into {} records'.format(num_events,
                                                       len(records)))
    logging.info('Sorting {} records'.format(len(records)))
    records.sort(key=lambda x: x.time)
    write_records(records)

def _parse_args():
    parser = argparse.ArgumentParser(description='Generate CSV data files.')
    parser.add_argument('events_file',
                        help='gzipped events file')
    parser.add_argument('-f', '--from-date', dest='from_date',
                        default=None,
                        help='only from the given RFC3339 date')
    return parser.parse_args()

def main():
    log_format = '%(asctime)-15s %(levelname)s %(message)s'
    date_format = '%Y%m%d %H:%M:%S'
    logging.basicConfig(level=logging.INFO,
                        format=log_format,
                        datefmt=date_format)
    args = _parse_args()
    process_file(args.events_file, from_timestamp=args.from_date)

if __name__ == "__main__":
    main()
