import unittest
import tempfile
import os


import semserver.locations as locations


class TestLocationIndex(unittest.TestCase):

    def test_save_to_files(self):
        data = [
            (locations.Location(-5.0001, 0.0), 'u1', 501),
            (locations.Location(-5.0002, -0.00001), 'u2', 502),
            (locations.Location(-4.9999, 0.00001), 'u3', 503),
            (locations.Location(-5.3, 0.3), 'u4', 504),
            (locations.Location(-5.4, 0.4), 'u5', 505),
            (locations.Location(-5.5, 0.5), 'u6', 506),
        ]
        index = locations.LocationIndex(500.0)
        for datum in data:
            index.insert(*datum)
        try:
            f_data, filename_data = tempfile.mkstemp(suffix='-data')
            f_loc, filename_locations = tempfile.mkstemp(suffix='-locations')
            os.close(f_data)
            os.close(f_loc)
            index.dump_to_files(filename_data, filename_locations)
            index2 = locations.LocationIndex(500.0)
            index2.load_from_files(filename_data, filename_locations)
            result = list(index2.lookup(locations.Location(-5.0, 0.0), 'u2'))
            self.assertEqual(len(result), 2)
            if result[0][1] == 501:
                self.assertEqual(result[0][0].lat, -5.0001)
                self.assertEqual(result[0][0].long, 0.0)
                self.assertEqual(result[1][0].lat, -4.9999)
                self.assertEqual(result[1][0].long, 0.00001)
                self.assertEqual(result[1][1], 503)
            elif result[0][1] == 503:
                self.assertEqual(result[0][0].lat, -4.9999)
                self.assertEqual(result[0][0].long, 0.00001)
                self.assertEqual(result[1][0].lat, -5.0001)
                self.assertEqual(result[1][0].long, 0.0)
                self.assertEqual(result[1][1], 501)
            else:
                raise RuntimeError('Wrong data')
        finally:
            os.remove(filename_data)
            os.remove(filename_locations)
