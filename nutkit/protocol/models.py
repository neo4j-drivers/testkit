
"""
All models are sent from backend as:
    {
        name: <class name>
        data: {
            <all instance variables>
        }
    }
"""

class Driver:
    def __init__(self, id="<invalidid>"):
        self.id = id


class Session:
    def __init__(self, id="invalidid>"):
        self.id = id


class Result:
    def __init__(self, id="invalidid>"):
        self.id = id


