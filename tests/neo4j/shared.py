"""
Shared utilities for writing tests against Neo4j server

Uses environment variables for configuration:

TEST_NEO4J_HOST    Neo4j server host, no default, required
TEST_NEO4J_PORT    Neo4j server port, default is 7687
"""
import os
from nutkit.frontend import Driver
from nutkit.protocol import AuthorizationToken


env_neo4j_host = "TEST_NEO4J_HOST"
env_neo4j_user = "TEST_NEO4J_USER"
env_neo4j_pass = "TEST_NEO4J_PASS"
env_neo4j_sheme = "TEST_NEO4J_SCHEME"


def get_authorization():
    """ Returns default authorization for tests that do not test this aspect
    """
    user = os.environ.get(env_neo4j_user, 'neo4j')
    passw = os.environ.get(env_neo4j_pass, 'pass')
    return AuthorizationToken(scheme="basic", principal=user, credentials=passw)


def get_neo4j_host_and_port():
    host = os.environ.get(env_neo4j_host)
    if not host:
        raise Exception("Missing Neo4j hostname, set %s" % env_neo4j_host)
    port = os.environ.get('TEST_NEO4J_PORT', 7687)
    return (host, port)


def get_neo4j_scheme():
    scheme = os.environ.get(env_neo4j_sheme, "bolt")
    return scheme


def get_driver(backend):
    """ Returns default driver for tests that do not test this aspect
    """
    scheme = get_neo4j_scheme()
    host, port = get_neo4j_host_and_port()
    uri = "%s://%s:%d" % (scheme, host, port)
    return Driver(backend, uri, get_authorization())
