from __future__ import unicode_literals

import gzip
import collections
import argparse

import ztreamy

group_names = {
    'Standard Deviation Heart Rate Section':
        'std_deviation_heart_rate_section',
    'High Deceleration': 'high_deceleration',
    'High Heart Rate': 'high_heart_rate',
    'Standard Deviation of Vehicle Speed Section':
        'std_deviation_vehicle_speed_section',
    'Positive Kinetic Energy': 'positive_kinetic_energy',
    'Average Speed Section': 'average_speed_section',
    'High Acceleration': 'high_acceleration',
    'Low Speed': 'low_speed',
    'High Speed': 'high_speed',
    'Heart Rate Section': 'heart_rate_section',
    'Stops Section': 'stops_section',
}

def read_file(filename):
    events = []
    with gzip.GzipFile(filename, 'r') as f:
        for e in ztreamy.Deserializer().deserialize_file(f):
            events.extend(e)
    return events

def clean(events, from_date=None):
    checked_events = []
    for event in events:
        if not event.timestamp.endswith('+02:00'):
            event.timestamp = event.timestamp + '+02:00'
        try:
            ztreamy.rfc3339_as_time(event.timestamp)
        except ztreamy.ZtreamyException:
            pass
        else:
            checked_events.append(event)
    events = checked_events
    if from_date:
        from_date = ztreamy.rfc3339_as_time(from_date)
        events = [e for e in events \
                  if ztreamy.rfc3339_as_time(e.timestamp) >= from_date]
    return [e for e in events if e.application_id == 'SmartDriver']

def group(events):
    groups = collections.defaultdict(list)
    for event in events:
        groups[event.body.keys()[0]].append(event)
    return groups

def extract_data(event, name):
    data = event.body[name]
    row = [
        name, event.timestamp,
        data['latitude'], data['longitude'],
        data['orientation'],
        data['value'],
    ]
    if 'distancia' in data:
        row.append(data['distancia'])
    else:
        row.append('')
    return row

def write_groups(groups):
    all_data = []
    for name in group_names:
        data = []
        for event in sorted(groups[name],
                           key=lambda x: ztreamy.rfc3339_as_time(x.timestamp)):
            data.append(extract_data(event, name))
        with open(group_names[name] + '.dat', mode='w') as f:
            for row in data:
                f.write(','.join([str(d) for d in row]))
                f.write('\n')
        all_data.extend(data)
    with open('all.dat', mode='w') as f:
        for row in sorted(all_data,
                          key=lambda x: ztreamy.rfc3339_as_time(x[1])):
            f.write(','.join([str(d) for d in row]))
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
    groups = group(clean(read_file(args.events_file),
                         from_date=args.from_date))
    write_groups(groups)

if __name__ == "__main__":
    main()
