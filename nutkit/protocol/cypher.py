"""
Represents types that is part of the bolt/cypher protocol that needs to be sent
as parameters to queries (from frontend to backend) and data retrieved as a
result from running a query (from backend to frontend). The values in record
response has instances of these types.

All cypher types are sent from backend as:
    {
        name: <class name>
        data: {
            <all instance variables>
        }
    }
"""


class CypherNull:
    """ Represents null/nil as sent/received to/from the database
    """

    def __init__(self, value=None):
        self.value = None

    def __str__(self):
        return "<null>"

    def __eq__(self, other):
        return isinstance(other, CypherNull)


class CypherList:
    def __init__(self, value):
        self.value = value

    def __str__(self):
        v = []
        for x in self.value:
            v.append(str(x))
        return "List {}".format(v)

    def __eq__(self, other):
        return isinstance(other, CypherList) and other.value == self.value


class CypherMap:
    def __init__(self, value):
        self.value = value

    def __str__(self):
        v = {}
        for k in self.value:
            v[k] = str(self.value[k])
        return "Map {}".format(v)

    def __eq__(self, other):
        return isinstance(other, CypherMap) and other.value == self.value


class CypherInt:
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return str(self.value)

    def __eq__(self, other):
        return isinstance(other, CypherInt) and other.value == self.value


class CypherBool:
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return str(self.value)

    def __eq__(self, other):
        return isinstance(other, CypherBool) and other.value == self.value


class CypherFloat:
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return str(self.value)

    def __eq__(self, other):
        return isinstance(other, CypherFloat) and other.value == self.value


class CypherString:
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return self.value

    def __eq__(self, other):
        return isinstance(other, CypherString) and other.value == self.value


class Node:
    def __init__(self, id, labels, props):
        self.id = id
        self.labels = labels
        self.props = props

    def __str__(self):
        return "Node (id={}, labels={}), props={}".format(
            self.id, self.labels, self.props)

    def __eq__(self, other):
        if not isinstance(other, Node):
            return False

        return self.id == other.id and self.labels == other.labels and self.props == other.props


# More in line with other naming
CypherNode = Node
