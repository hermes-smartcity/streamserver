import random


class DriverFeedback(object):
    def __init__(self):
        self.recommendation = DriverRecommendation()
        self.scores = CloseScores()

    def as_dict(self):
        return {
            'recommendation': self.recommendation.as_dict(),
            'scores': self.scores.as_dict(),
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


def fake_driver_score(base_latitude, base_longitude):
    return DriverScore(base_latitude + random.uniform(-0.005, 0.005),
                       base_longitude + random.uniform(-0.005, 0.005),
                       random.randint(0, 1000))

def fake_feedback(base_latitude=40.339300, base_longitude=-3.773988):
    feedback = DriverFeedback()
    feedback.scores.add_score(fake_driver_score(base_latitude, base_longitude))
    feedback.scores.add_score(fake_driver_score(base_latitude, base_longitude))
    return feedback
