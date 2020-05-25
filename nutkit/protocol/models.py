
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
    def __init__(self, id):
        self.id = id


class Session:
    def __init__(self, id):
        self.id = id


class Result:
    def __init__(self, id):
        self.id = id


class Record:
    def __init__(self, values=None):
        self.values = values


class NullRecord:
    def __init__(self):
        pass


class Error:
    def __init__(self):
        pass


class CypherNull:
    def __init__(self):
        pass


class CypherList:
    def __init__(self, value):
        self.value = value


class CypherInt:
    def __init__(self, value):
        self.value = value


class CypherString:
    def __init__(self, value):
        self.value = value

