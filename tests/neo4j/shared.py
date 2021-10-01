"""
Shared utilities for writing tests against Neo4j server

Uses environment variables for configuration:

TEST_NEO4J_HOST    Neo4j server host, no default, required
TEST_NEO4J_PORT    Neo4j server port, default is 7687
"""
import os
from warnings import warn

from nutkit.frontend import Driver
from nutkit.protocol import AuthorizationToken
from tests.shared import (
    dns_resolve_single,
    TestkitTestCase,
)


env_neo4j_host = "TEST_NEO4J_HOST"
env_neo4j_user = "TEST_NEO4J_USER"
env_neo4j_pass = "TEST_NEO4J_PASS"
env_neo4j_scheme = "TEST_NEO4J_SCHEME"
env_neo4j_bolt_port = "TEST_NEO4J_PORT"
env_neo4j_http_port = "TEST_NEO4J_HTTP_PORT"
env_neo4j_version = "TEST_NEO4J_VERSION"
env_neo4j_edition = "TEST_NEO4J_EDITION"
env_neo4j_cluster = "TEST_NEO4J_CLUSTER"


def get_authorization():
    """ Returns default authorization for tests that do not test this aspect
    """
    user = os.environ.get(env_neo4j_user, 'neo4j')
    passw = os.environ.get(env_neo4j_pass, 'pass')
    return AuthorizationToken("basic", principal=user, credentials=passw)


def get_neo4j_host_and_port():
    host = os.environ.get(env_neo4j_host)
    if not host:
        raise Exception("Missing Neo4j hostname, set %s" % env_neo4j_host)
    port = int(os.environ.get(env_neo4j_bolt_port, 7687))
    return host, port


def get_neo4j_resolved_host_and_port():
    host, port = get_neo4j_host_and_port()
    return dns_resolve_single(host), port


def get_neo4j_host_and_http_port():
    host = os.environ.get(env_neo4j_host)
    if not host:
        raise Exception("Missing Neo4j hostname, set %s" % env_neo4j_host)
    port = os.environ.get(env_neo4j_http_port, 17401)
    return host, port


def get_neo4j_scheme():
    scheme = os.environ.get(env_neo4j_scheme, "bolt")
    return scheme


def get_driver(backend, uri=None, auth=None, **kwargs):
    """ Returns default driver for tests that do not test this aspect
    """
    if uri is None:
        scheme = get_neo4j_scheme()
        host, port = get_neo4j_host_and_port()
        uri = "%s://%s:%d" % (scheme, host, port)
    if auth is None:
        auth = get_authorization()
    return Driver(backend, uri, auth, **kwargs)


class ServerInfo:
    def __init__(self, version: str, edition: str, cluster: bool):
        self.version = version
        self.edition = edition
        self.cluster = cluster

    @property
    def supports_multi_db(self):
        return self.version >= "4" and self.edition == "enterprise"

    @property
    def max_protocol_version(self):
        return {
            "3.5": "3.0",
            "4.0": "4.0",
            "4.1": "4.1",
            "4.2": "4.2",
            "4.3": "4.3",
            "4.4": "4.4",
        }[".".join(self.version.split(".")[:2])]


def get_server_info():
    return ServerInfo(
        version=os.environ.get(env_neo4j_version, "4.3"),
        edition=os.environ.get(env_neo4j_edition, "enterprise"),
        cluster=(os.environ.get(env_neo4j_cluster, "False").lower()
                 in ("true", "yes", "y", "1"))
    )


def cluster_unsafe_test(func):
    def wrapper(*args, **kwargs):
        if len(args) >= 1 and isinstance(args[0], TestkitTestCase):
            if get_server_info().cluster:
                args[0].skipTest("Test does not support cluster")
            else:
                return func(*args, **kwargs)
        else:
            warn("cluster_unsafe_test should only be used to decorate "
                 "TestkitTestCase methods.")
            return func(*args, **kwargs)
    return wrapper
