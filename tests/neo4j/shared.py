"""
Shared utilities for writing tests against Neo4j server

Uses environment variables for configuration:

TEST_NEO4J_HOST    Neo4j server host, no default, required
TEST_NEO4J_PORT    Neo4j server port, default is 7687
"""
import os
from nutkit.frontend import Driver, AuthorizationToken


env_neo4j_host = "TEST_NEO4J_HOST"

def get_authorization():
    """ Returns default authorization for tests that do not test this aspect
    """
    return AuthorizationToken(scheme="basic", principal="neo4j", credentials="pass")


def get_neo4j_host_and_port():
    host = os.environ.get(env_neo4j_host)
    if not host:
        raise Exception("Missing Neo4j hostname, set %s" % env_neo4j_host)
    port = os.environ.get('TEST_NEO4J_PORT', 7687)
    return (host, port)


def get_driver(backend):
    """ Returns default driver for tests that do not test this aspect
    """
    host, port = get_neo4j_host_and_port()
    scheme = "bolt://%s:%d" % (host, port)
    return Driver(backend, scheme, get_authorization())

