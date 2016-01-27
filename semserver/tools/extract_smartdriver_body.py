from __future__ import unicode_literals

import sys
import gzip
import collections
import argparse
import re
import copy

import ztreamy


def read_file(filename):
    events = []
    with gzip.GzipFile(filename, 'r') as f:
        for events in ztreamy.Deserializer().deserialize_file(f):
            for event in events:
                yield event

def extract_data(filename):
    for event in read_file(filename):
        if event.application_id == 'SmartDriver':
            body = event.as_json()
            if body is not None and not '\n' in body:
                print(len(body))
                print(body)

def _parse_args():
    parser = argparse.ArgumentParser(description='Generate CSV data files.')
    parser.add_argument('events_file',
                        help='gzipped events file')
    ## parser.add_argument('-f', '--from-date', dest='from_date',
    ##                     default=None,
    ##                     help='only from the given RFC3339 date')
    return parser.parse_args()

def main():
    args = _parse_args()
    extract_data(args.events_file)

if __name__ == "__main__":
    main()
