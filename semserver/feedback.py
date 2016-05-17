import random

from . import locations


class Status(object):
    OK = 1
    DISABLED = 11
    USE_PREVIOUS = 21
    NO_DATA = 22
    SERVICE_TIMEOUT = 31
    SERVICE_ERROR = 32


class DriverFeedback(object):
    def __init__(self):
        self.recommendation = DriverRecommendation()
        self.scores = CloseScores()
        self.road_info = RoadInfo()

    def no_data(self, status):
        self.scores.no_data(status)
        self.road_info.no_data(status)

    def timeout(self):
        if  self.scores.status is None:
            self.scores.no_data(Status.SERVICE_TIMEOUT)
        if self.road_info.status is None:
            self.road_info.no_data(Status.SERVICE_TIMEOUT)

    def as_dict(self):
        return {
            'recommendation': self.recommendation.as_dict(),
            'scores': self.scores.as_dict(),
            'roadInfo': self.road_info.as_dict(),
        }


class DriverRecommendation(object):
    def __init__(self):
        pass

    def as_dict(self):
        return {}


class CloseScores(object):
    def __init__(self):
        self.scores = []
        self.status = None

    def add_score(self, score):
        self.scores.append(score)

    def no_data(self, status):
        self.status = status

    def status_ok(self):
        self.status = Status.OK

    def as_dict(self):
        if self.status is None:
            raise ValueError('Uninitialized status value '
                             'in CloseScores object')
        return {
            'status': self.status,
            'closeScores': [score.as_dict() for score in self.scores],
        }

    def load_from_lines(self, lines):
        self.status = Status.OK
        for line in lines:
            if line:
                parts = [p.strip() for p in line.split(',')]
                if len(parts) != 3:
                    raise ValueError('Malformed score line')
                self.add_score(DriverScore(float(parts[0]),
                                           float(parts[1]),
                                           int(parts[2])))


class RoadInfo(object):
    def __init__(self):
        self.status = None
        self.road_type = None
        self.max_speed = None

    def set_data(self, road_type, max_speed):
        self.status = Status.OK
        self.road_type = road_type
        self.max_speed = max_speed

    def no_data(self, status):
        self.status = status
        self.road_type = None
        self.max_speed = None

    def as_dict(self):
        if self.status is None:
            raise ValueError('Uninitialized status value in RoadInfo object')
        data = {'status': self.status}
        if self.status == Status.OK:
            data['roadType'] = self.road_type
            data['maxSpeed'] = self.max_speed
        return data


class DriverScore(object):
    def __init__(self, latitude, longitude, score):
        self.latitude = latitude
        self.longitude = longitude
        self.score = score

    def as_dict(self):
        return {
            'longitude': self.longitude,
            'latitude': self.latitude,
            'score': self.score,
        }


def fake_driver_score(base):
    return DriverScore(base.lat + random.uniform(-0.005, 0.005),
                       base.long + random.uniform(-0.005, 0.005),
                       random.randint(0, 1000))

def fake_scores(feedback_obj, base=locations.Location(40.339300, -3.773988)):
    feedback_obj.scores.add_score(fake_driver_score(base))
    feedback_obj.scores.add_score(fake_driver_score(base))
