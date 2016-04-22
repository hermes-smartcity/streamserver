from __future__ import print_function, division, unicode_literals

import collections
import math
import sqlite3
import time


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
        return math.acos(math.sin(self.lat_r) * math.sin(other.lat_r)
                         + math.cos(self.lat_r) * math.cos(other.lat_r)
                         * math.cos(self.long_r - other.long_r)) * _R

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

    @staticmethod
    def from_radians(lat_r, long_r):
        """ Returns a Location object from lat and long in radians."""
        return Location(lat_r * 180 / math.pi, long_r * 180 / math.pi)


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
        ORDER BY timestamp DESC"""

    def __init__(self, search_radius, ttl=600):
        """ Create a new index.

        The parameter `search_radius` should contain the radius
        for lookups in meters. The parameter `ttl` specifies
        that location data with more than that age in seconds should
        be dropped at the next `roll()` operation.

        """
        self.search_radius = search_radius
        self.ttl = ttl
        self.conn = sqlite3.connect(':memory:')
        self._create_tables()
        self.next_id = 1

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

    def lookup(self, location):
        cursor = self.conn.cursor()
        users = set()
        for row in cursor.execute(self._query_lookup,
                                  (location.lat, location.long)):
            if not row[2] in users:
                users.add(row[2])
                yield (row[0], row[1], row[3])

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
