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

    def __repr__(self):
        return "<{}>".format(self.__class__.__name__)

    def __eq__(self, other):
        return isinstance(other, CypherNull)


class CypherList:
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return str(list(map(str, self.value)))

    def __repr__(self):
        return "<{}({})>".format(self.__class__.__name__, self.__str__())

    def __eq__(self, other):
        return isinstance(other, CypherList) and other.value == self.value


class CypherMap:
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return str({k: str(str(self.value[k])) for k in self.value})

    def __repr__(self):
        return "<{}({})>".format(self.__class__.__name__, self.__str__())

    def __eq__(self, other):
        return isinstance(other, CypherMap) and other.value == self.value


class CypherInt:
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return str(self.value)

    def __repr__(self):
        return "<{}({})>".format(self.__class__.__name__, self.__str__())

    def __eq__(self, other):
        return isinstance(other, CypherInt) and other.value == self.value


class CypherBool:
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return str(self.value)

    def __repr__(self):
        return "<{}({})>".format(self.__class__.__name__, self.__str__())

    def __eq__(self, other):
        return isinstance(other, CypherBool) and other.value == self.value


class CypherFloat:
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return str(self.value)

    def __repr__(self):
        return "<{}({})>".format(self.__class__.__name__, self.__str__())

    def __eq__(self, other):
        return isinstance(other, CypherFloat) and other.value == self.value


class CypherString:
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return self.value

    def __repr__(self):
        return "<{}({})>".format(self.__class__.__name__, self.__str__())

    def __eq__(self, other):
        return isinstance(other, CypherString) and other.value == self.value


class Node:
    def __init__(self, id, labels, props):
        self.id = id
        self.labels = labels
        self.props = props

    def __str__(self):
        return "Node(id={}, labels={}, props={})".format(
            self.id, self.labels, self.props)

    def __repr__(self):
        return "<{}(id={}, labels={}, props={})>".format(
            self.__class__.__name__, self.id, self.labels, self.props
        )

    def __eq__(self, other):
        if not isinstance(other, Node):
            return False
        return all(getattr(self, attr) == getattr(other, attr)
                   for attr in ("id", "labels", "props"))


# More in line with other naming
CypherNode = Node


class Relationship:
    def __init__(self, id, startNodeId, endNodeId, type, props):
        self.id = id
        self.startNodeId = startNodeId
        self.endNodeId = endNodeId
        self.type = type
        self.props = props

    def __str__(self):
        return (
            "Relationship(id={}, startNodeId={}, endNodeId={}, type={}, "
            "props={})".format(self.id, self.startNodeId, self.endNodeId,
                               self.type, self.props)
        )

    def __repr__(self):
        return (
            "<{}(id={}, startNodeId={}, endNodeId={}, type={}, "
            "props={})>".format(self.__class__.__name__, self.id,
                                self.startNodeId, self.endNodeId, self.type,
                                self.props)
        )

    def __eq__(self, other):
        if not isinstance(other, Relationship):
            return False

        return all(getattr(self, attr) == getattr(other, attr)
                   for attr in ("id", "startNodeId", "endNodeId", "type",
                                "props"))


# More in line with other naming
CypherRelationship = Relationship


class Path:
    def __init__(self, nodes, relationships):
        self.nodes = nodes
        self.relationships = relationships

    def __str__(self):
        return "Path(nodes={}, relationships={})".format(
            self.nodes, self.relationships
        )

    def __repr__(self):
        return "<{}(nodes={}, relationships={})>".format(
            self.__class__.__name__, self.nodes, self.relationships
        )

    def __eq__(self, other):
        if not isinstance(other, Path):
            return False

        return all(getattr(self, attr) == getattr(other, attr)
                   for attr in ("nodes", "relationships"))


# More in line with other naming
CypherPath = Path
