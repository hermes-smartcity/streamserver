import random

from . import locations


class DriverFeedback(object):
    def __init__(self):
        self.recommendation = DriverRecommendation()
        self.scores = CloseScores()
        self.road_info = {}

    def as_dict(self):
        return {
            'recommendation': self.recommendation.as_dict(),
            'scores': self.scores.as_dict(),
            'roadInfo': self.road_info,
        }


class DriverRecommendation(object):
    def __init__(self):
        pass

    def as_dict(self):
        return {}


class CloseScores(object):
    def __init__(self):
        self.scores = []

    def add_score(self, score):
        self.scores.append(score)

    def as_dict(self):
        return {
            'closeScores': [score.as_dict() for score in self.scores],
        }


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
