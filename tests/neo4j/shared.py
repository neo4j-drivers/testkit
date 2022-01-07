"""
Shared utilities for writing tests against Neo4j server.

Uses environment variables for configuration:

TEST_NEO4J_SCHEME    Scheme to build the URI when contacting the Neo4j server,
                     default "bolt"
TEST_NEO4J_HOST      Neo4j server host, no default, required
TEST_NEO4J_PORT      Neo4j server port, default is 7687
TEST_NEO4J_USER      User to access the Neo4j server, default "neo4j"
TEST_NEO4J_PASS      Password to access the Neo4j server, default "pass"
TEST_NEO4J_VERSION   Version of the Neo4j server, default "4.4"
TEST_NEO4J_EDITION   Edition ("enterprise", "community", or "aura") of the
                     Neo4j server, default "enterprise"
TEST_NEO4J_CLUSTER   Whether the Neo4j server is a cluster, default "False"
"""
from functools import wraps
import os
from warnings import warn

from nutkit import protocol
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
    """Return default authorization for tests that do not test this aspect."""
    user = os.environ.get(env_neo4j_user, "neo4j")
    passw = os.environ.get(env_neo4j_pass, "pass")
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
    """Return default driver for tests that do not test this aspect."""
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
    def server_agent(self):
        if self.edition == "aura":
            raise ValueError(
                "We can't predict the server's agent string for aura!"
            )
        return "Neo4j/" + self.version

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
            "5.0": "5.0",
        }[".".join(self.version.split(".")[:2])]


def get_server_info():
    return ServerInfo(
        version=os.environ.get(env_neo4j_version, "4.4"),
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


def requires_multi_db_support(func):
    def get_valid_test_case(*args, **kwargs):
        if not args or not isinstance(args[0], TestkitTestCase):
            raise TypeError("Should only decorate TestkitTestCase methods")
        return args[0]

    @wraps(func)
    @requires_min_bolt_version(protocol.Feature.BOLT_4_0)
    def wrapper(*args, **kwargs):
        test_case = get_valid_test_case(*args, **kwargs)
        if not get_server_info().supports_multi_db:
            test_case.skipTest("Server does not support multiple databases.")
        return func(*args, **kwargs)
    return wrapper


def requires_min_bolt_version(feature):
    if not isinstance(feature, protocol.Feature):
        raise TypeError("The arguments must be instances of Feature")
    if not feature.name.startswith("BOLT_"):
        raise ValueError("Bolt version feature expected")

    min_version = feature.value.split(":")[-1]
    server_max_version = get_server_info().max_protocol_version
    all_viable_versions = [
        f for f in protocol.Feature
        if (f.value.startswith("BOLT_")
            and min_version <= f.value.spit(":")[-1] <= server_max_version)
    ]

    def get_valid_test_case(*args, **kwargs):
        if not args or not isinstance(args[0], TestkitTestCase):
            raise TypeError("Should only decorate TestkitTestCase methods")
        return args[0]

    def bolt_version_decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            test_case = get_valid_test_case(*args, **kwargs)
            if server_max_version < min_version:
                test_case.skipTest("Server does not support minimum required "
                                   "Bolt version: " + min_version)
            missing = test_case.driver_missing_features(*all_viable_versions)
            if len(missing) == len(all_viable_versions):
                test_case.skipTest("There is no common version between server "
                                   "and driver that fulfills the minimum "
                                   "required protocol version: " + min_version)
            return func(*args, **kwargs)
        return wrapper
    return bolt_version_decorator
