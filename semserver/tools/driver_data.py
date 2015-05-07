from __future__ import unicode_literals

import gzip
import collections
import sys

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
    with gzip.GzipFile(filename, 'r') as f:
        data = f.read()
    return ztreamy.Deserializer().deserialize(data, complete=True)

def clean(events):
    events = events[7691:]
    for event in events:
        if not event.timestamp.endswith('+02:00'):
            event.timestamp = event.timestamp + '+02:00'
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
        for event in groups[name]:
            data.append(extract_data(event, name))
        with open(group_names[name] + '.dat', mode='w') as f:
            for row in data:
                f.write(','.join([str(d) for d in row]))
                f.write('\n')
        all_data.extend(data)
    with open('all.dat', mode='w') as f:
        for row in all_data:
            f.write(','.join([str(d) for d in row]))
            f.write('\n')

def main():
    filename = sys.argv[1]
    groups = group(clean(read_file(filename)))
    write_groups(groups)

if __name__ == "__main__":
    main()
