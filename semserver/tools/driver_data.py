from __future__ import unicode_literals

import sys
import gzip
import collections
import argparse
import re
import copy

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


class Record(object):
    def __init__(self, user_id):
        self.user_id = user_id
        self.record_type = None
        self.timestamp_ini = None
        self.timestamp = None
        self.latitude = None
        self.latitude_ini = None
        self.longitude = None
        self.longitude_ini = None
        self.value = None
        self.mean_value = None
        self.median_value = None
        self.std_dev = None
        self.min_value = None
        self.max_value = None

    @property
    def time(self):
        return ztreamy.rfc3339_as_time(self.timestamp)

    def as_row(self):
        self.check()
        return self._as_row()

    def _as_row(self):
        return [str(v) if v is not None else '' \
                for v in (self.record_type,
                          self.user_id,
                          self.timestamp,
                          self.latitude,
                          self.longitude,
                          self.timestamp_ini,
                          self.latitude_ini,
                          self.longitude_ini,
                          self.value,
                          self.mean_value,
                          self.median_value,
                          self.std_dev,
                          self.min_value,
                          self.max_value)]

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


def read_file(filename):
    events = []
    with gzip.GzipFile(filename, 'r') as f:
        for e in ztreamy.Deserializer().deserialize_file(f):
            events.extend(e)
    return events

_tz_re = re.compile(r'.*\+\d\d:\d\d$')

def fix_timestamp(timestamp, latitude=None, longitude=None):
    if not _tz_re.match(timestamp):
        # No time zone information. Guess it.
        if latitude is not None:
            if latitude < 47:
                mod_timestamp = timestamp + '+01:00'
            else:
                mod_timestamp = timestamp + '+00:00'
        else:
                mod_timestamp = timestamp + '+01:00'
    else:
        mod_timestamp = timestamp
    try:
        ztreamy.rfc3339_as_time(mod_timestamp)
    except ztreamy.ZtreamyException:
        raise ValueError('Bad timestamp: ' + timestamp)
    return mod_timestamp

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
            record = Record(event.source_id)
            record.record_type = 'Vehicle Location'
            record.latitude = sample['latitude']
            record.longitude = sample['longitude']
            record.timestamp = fix_timestamp(sample['timeStamp'])
            records.append(record)
    return records

def extract_simple_event(event):
    event_type = event.event_type
    if event_type is not None and event_type in simple_event_types:
        data = event.body[event_type]
        record = Record(event.source_id)
        record.record_type = event_type
        record.timestamp = fix_timestamp(event.timestamp)
        record.value = data['value']
        record.latitude = data['latitude']
        record.longitude = data['longitude']
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

def events_to_records(events, from_timestamp=None):
    records = []
    for event in events:
        records.extend(extract_data(event))
    records.sort(key=lambda x: x.time)
    if from_timestamp is not None:
        from_time = ztreamy.rfc3339_as_time(from_timestamp)
        records = [r for r in records if r.time >= from_time]
    return records

def write_records(records):
    with open('data.csv', mode='w') as f:
        for record in records:
            f.write(','.join(record.as_row()))
            f.write('\n')

def _parse_args():
    parser = argparse.ArgumentParser(description='Generate CSV data files.')
    parser.add_argument('events_file',
                        help='gzipped events file')
    parser.add_argument('-f', '--from-date', dest='from_date',
                        default=None,
                        help='only from the given RFC3339 date')
    return parser.parse_args()

def main():
    args = _parse_args()
    events = read_file(args.events_file)
    events = [e for e in events if e.source_id not in ignore_source_ids]
    records = events_to_records(events, from_timestamp=args.from_date)
    write_records(records)

if __name__ == "__main__":
    main()
