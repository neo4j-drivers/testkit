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
    return AuthorizationToken(scheme="basic", principal=user, credentials=passw)


def get_neo4j_host_and_port():
    host = os.environ.get(env_neo4j_host)
    if not host:
        raise Exception("Missing Neo4j hostname, set %s" % env_neo4j_host)
    port = os.environ.get(env_neo4j_bolt_port, 7687)
    return host, port


def get_neo4j_host_and_http_port():
    host = os.environ.get(env_neo4j_host)
    if not host:
        raise Exception("Missing Neo4j hostname, set %s" % env_neo4j_host)
    port = os.environ.get(env_neo4j_http_port, 17401)
    return host, port


def get_neo4j_scheme():
    scheme = os.environ.get(env_neo4j_scheme, "bolt")
    return scheme


def get_driver(backend, uri=None, **kwargs):
    """ Returns default driver for tests that do not test this aspect
    """
    if uri is None:
        scheme = get_neo4j_scheme()
        host, port = get_neo4j_host_and_port()
        uri = "%s://%s:%d" % (scheme, host, port)
    return Driver(backend, uri, get_authorization(), **kwargs)


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
        }[".".join(self.version.split(".")[:2])]


def get_server_info():
    return ServerInfo(
        version=os.environ.get(env_neo4j_version, "4.3"),
        edition=os.environ.get(env_neo4j_edition, "enterprise"),
        cluster=(os.environ.get(env_neo4j_cluster, "False").lower()
                 in ("true", "yes", "y", "1"))
    )
