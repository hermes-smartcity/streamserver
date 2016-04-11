from __future__ import print_function, division, unicode_literals

import collections
import math


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
