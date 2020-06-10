
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


class RetryableTry:
    def __init__(self, id):
        self.id = id

class RetryableDone:
    def __init__(self):
        pass

class Error:
    def __init__(self, id):
        pass


class CypherNull:
    def __init__(self):
        pass


class CypherList:
    def __init__(self, value):
        self.value = value

    def __str__(self):
        v = []
        for x in self.value:
            v.append(str(x))
        return "List {}".format(v)

class CypherMap:
    def __init__(self, value):
        self.value = value

    def __str__(self):
        v = {}
        for k in self.value:
            v[k] = str(self.value[k])
        print(self.value)
        return "Map {}".format(v)


class CypherInt:
    def __init__(self, value):
        self.value = value


class CypherString:
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return self.value


class Node:
    def __init__(self, id, labels, props):
        self.id = id
        self.labels = labels
        self.props = props

    def __str__(self):
        return "Node (id={}, labels={}), props={}".format(
            self.id, self.labels, self.props)



