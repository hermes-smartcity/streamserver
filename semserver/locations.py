from __future__ import print_function, division, unicode_literals

import collections
import math
import sqlite3
import time
import logging


_R = 6371000


class Location(collections.namedtuple('Location', ('lat', 'long'),
                                      verbose=False)):
    @property
    def lat_r(self):
        return self.lat * math.pi / 180

    @property
    def long_r(self):
        return self.long * math.pi / 180

    def distance(self, other):
        """Distance in m between two locations."""
        v = (math.sin(self.lat_r) * math.sin(other.lat_r)
             + math.cos(self.lat_r) * math.cos(other.lat_r)
             * math.cos(self.long_r - other.long_r))
        if v > 1:
            # Prevent precision error (for equal points the value may
            # be something like 1.0000000000000002)
            return 0.0
        elif v < -1:
            return math.pi * _R
        else:
            return math.acos(v) * _R

    def bounding_box(self, radius):
        """ Return the smallest rectangle that contais a circle around.

        The `radius` parameters contains the radius of the circle in meters.
        Returns a tuple of two locations (corners top-left and bottom-right)
        of the rectangle.

        Based on:
        http://janmatuschek.de/LatitudeLongitudeBoundingCoordinates

        """
        r = radius / _R
        delta_long = math.asin(math.sin(r) / math.cos(self.lat_r))
        return (self.from_radians(self.lat_r - r, self.long_r - delta_long),
                self.from_radians(self.lat_r + r, self.long_r + delta_long))

    def __str__(self):
        return '{},{}'.format(self.lat, self.long)

    @staticmethod
    def from_radians(lat_r, long_r):
        """ Returns a Location object from lat and long in radians."""
        return Location(lat_r * 180 / math.pi, long_r * 180 / math.pi)

    @staticmethod
    def parse(text):
        """ Returns a Location object from a str as written by __str__."""
        parts = text.split(',')
        if len(parts) == 2:
            return Location(float(parts[0]), float(parts[1]))
        else:
            raise ValueError('Syntax error in Location object')


class LocationIndex(object):
    _table_definitions = [
        """
        CREATE TABLE Data (
            id INTEGER PRIMARY KEY NOT NULL,
            lat DOUBLE NOT NULL,
            long DOUBLE NOT NULL,
            user_id TEXT NOT NULL,
            score INTEGER NOT NULL,
            timestamp DOUBLE NOT NULL,
            FOREIGN KEY(id) REFERENCES Locations(id)
        )""",
        """
        CREATE VIRTUAL TABLE Locations USING rtree (
            id,
            lat_min,
            lat_max,
            long_min,
            long_max
        )""",
        """
        CREATE INDEX TimestampIndex ON Data(timestamp)""",
    ]

    _query_lookup = """
        SELECT lat, long, user_id, score
        FROM Locations INNER JOIN Data ON Data.id = Locations.id
        WHERE lat_min <= :1 AND lat_max >= :1
        AND long_min <= :2 and long_max >= :2
        AND user_id <> :3
        ORDER BY timestamp DESC"""

    _query_lookup_same_user = """
        SELECT lat, long, user_id, score
        FROM Locations INNER JOIN Data ON Data.id = Locations.id
        WHERE lat_min <= :1 AND lat_max >= :1
        AND long_min <= :2 and long_max >= :2
        AND (user_id <> :3 OR timestamp < :4)
        ORDER BY timestamp DESC"""

    def __init__(self, search_radius, ttl=600, allow_same_user=False):
        """ Create a new index.

        The parameter `search_radius` should contain the radius
        for lookups in meters. The parameter `ttl` specifies
        that location data with more than that age in seconds should
        be dropped at the next `roll()` operation.

        """
        self.search_radius = search_radius
        self.ttl = ttl
        if allow_same_user:
            # Replace the lookup method
            self.lookup = self._lookup_allow_same_user
        self.conn = sqlite3.connect(':memory:')
        self._create_tables()
        self.next_id = 1
        logging.debug(('Initialized LocationIndex, radius: {}m, '
                       'ttl: {}s, allow_same_user: {}')\
                      .format(search_radius, ttl, allow_same_user))

    def insert(self, location, user_id, score):
        top_left, bottom_right = location.bounding_box(self.search_radius)
        cursor = self.conn.cursor()
        cursor.execute("INSERT INTO Locations VALUES (?, ?, ?, ?, ?)",
                       (self.next_id, top_left.lat, bottom_right.lat,
                        top_left.long, bottom_right.long))
        cursor.execute("INSERT INTO Data VALUES (?, ?, ?, ?, ?, ?)",
                       (self.next_id, location.lat, location.long,
                        user_id, score, time.time()))
        self.conn.commit()
        self.next_id += 1
        ## logging.debug('Insert {}, {}, {}'.format(location.lat,
        ##                                          location.long,
        ##                                          time.time()))

    def lookup(self, location, user_id):
        cursor = self.conn.cursor()
        users = set()
        for row in cursor.execute(self._query_lookup,
                                  (location.lat, location.long,
                                   user_id)):
            if not row[2] in users:
                users.add(row[2])
                yield (Location(row[0], row[1]), row[3])

    def _lookup_allow_same_user(self, location, user_id):
        # This method replaces the lookup method in the constructor,
        # when configured.
        # Don't get results from the same user in the last hour
        timestamp_lim = time.time() - 3600.0
        cursor = self.conn.cursor()
        users = set()
        for row in cursor.execute(self._query_lookup_same_user,
                                  (location.lat, location.long,
                                   user_id, timestamp_lim)):
            if not row[2] in users:
                users.add(row[2])
                yield (Location(row[0], row[1]), row[3])

    def roll(self):
        cursor = self.conn.cursor()
        result = cursor.execute('SELECT MAX(id) FROM Data '
                                'WHERE ? - timestamp > ?',
                                (time.time(), self.ttl)).fetchone()
        if result is not None:
            min_id = result[0]
            cursor.execute('DELETE FROM Data WHERE id <= ?', (min_id, ))
            cursor.execute('DELETE FROM Locations WHERE id <= ?', (min_id, ))
            self.conn.commit()

    def _create_tables(self):
        cursor = self.conn.cursor()
        for table_decl in self._table_definitions:
            cursor.execute(table_decl)
        self.conn.commit()
