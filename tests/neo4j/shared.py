"""
Shared utilities for writing tests

Uses environment variables for configuration:

TEST_BACKEND_HOST  Hostname of backend, default is localhost
TEST_BACKEND_PORT  Port on backend host, default is 9876
TEST_NEO4J_HOST    Neo4j server host, no default, required
TEST_NEO4J_PORT    Neo4j server port, default is 7687
"""
import os
from nutkit.backend import Backend
from nutkit.frontend import Driver, AuthorizationToken


def get_backend_host_and_port():
    return (os.environ.get('TEST_BACKEND_HOST', 'localhost'), os.environ.get('TEST_BACKEND_PORT', 9876))


def get_neo4j_host_and_port():
    return (os.environ.get('TEST_NEO4J_HOST'), os.environ.get('TEST_NEO4J_PORT', 7687))


def newBackend():
    """ Returns connection to backend, caller is responsible for closing
    """
    host, port = get_backend_host_and_port()
    return Backend(host, port)


def getAuthorization():
    """ Returns default authorization for tests that do not test this aspect
    """
    return AuthorizationToken(scheme="basic", principal="neo4j", credentials="pass")


def getDriver(backend):
    """ Returns default driver for tests that do not test this aspect
    """
    host, port = get_neo4j_host_and_port()
    scheme = "bolt://%s:%d" % (host, port)
    return Driver(backend, scheme, getAuthorization())

