"""
Shared utilities for writing tests against Neo4j server.

Uses environment variables for configuration:

TEST_NEO4J_SCHEME      Scheme to build the URI when contacting the Neo4j
                       server, default "bolt"
TEST_NEO4J_HOST        Neo4j server host, no default, required
TEST_NEO4J_PORT        Neo4j server port, default is 7687
TEST_NEO4J_HTTP_PORT   Neo4j HTTP port, default is 7474
TEST_NEO4J_USER        User to access the Neo4j server, default "neo4j"
TEST_NEO4J_PASS        Password to access the Neo4j server, default "pass"
TEST_NEO4J_VERSION     Version of the Neo4j server, default "4.4"
TEST_NEO4J_EDITION     Edition ("enterprise", "community", or "aura") of the
                       Neo4j server, default "enterprise"
TEST_NEO4J_CLUSTER     Whether the Neo4j server is a cluster, default "False"
TEST_NEO4J_DEFAULT_DB  Default database name, default "neo4j"
"""

import os
import re
import traceback
from functools import wraps
from time import (
    sleep,
    time,
)
from warnings import warn

from nutkit import protocol
from nutkit.frontend import Driver
from nutkit.protocol import (
    AuthorizationToken,
    ClientCertificate,
)
from tests.shared import (
    dns_resolve_single,
    Potential,
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
env_neo4j_default_db = "TEST_NEO4J_DEFAULT_DB"
env_neo4j_client_cert = "TEST_NEO4J_SSL_CLIENT_CERT"
env_neo4j_client_key = "TEST_NEO4J_SSL_CLIENT_KEY"


_BOLT_SCHEME_RE = re.compile(r"^(?:bolt|neo4j)(?:\+s(?:sc)?)?$")


def get_authorization():
    """Return default authorization for tests that do not test this aspect."""
    return AuthorizationToken(
        "basic", principal=get_user(), credentials=get_password()
    )


def get_user():
    return os.environ.get(env_neo4j_user, "neo4j")


def get_password():
    return os.environ.get(env_neo4j_pass, "pass")


def get_neo4j_host_and_port():
    host = os.environ.get(env_neo4j_host)
    if not host:
        raise Exception("Missing Neo4j hostname, set %s" % env_neo4j_host)
    if get_server_info().is_http:
        port = get_http_port()
    else:
        port = get_bolt_port()
    return host, port


def get_neo4j_resolved_host_and_port():
    host, port = get_neo4j_host_and_port()
    return dns_resolve_single(host), port


def get_neo4j_host_and_wrong_port():
    host = os.environ.get(env_neo4j_host)
    if not host:
        raise Exception("Missing Neo4j hostname, set %s" % env_neo4j_host)
    return host, 17401


def get_bolt_port():
    return int(os.environ.get(env_neo4j_bolt_port, 7687))


def get_http_port():
    return int(os.environ.get(env_neo4j_http_port, 7474))


def get_neo4j_scheme():
    scheme = os.environ.get(env_neo4j_scheme, "bolt")
    return scheme


def get_default_db():
    return os.environ.get(env_neo4j_default_db, "neo4j")


def get_auto_resolved_db():
    if get_neo4j_scheme() in {"http", "https"}:
        # Via HTTP Query API, a database name *must* be specified
        return get_default_db()
    # Via Bolt, the driver can omit the database name.
    # The DBMS will use the user's home db or the system's default db.
    return None


def get_client_certificate():
    client_certificate_key = os.environ.get(env_neo4j_client_key)
    client_certificate_cert = os.environ.get(env_neo4j_client_cert)
    if client_certificate_cert is None or client_certificate_key is None:
        if (
            client_certificate_cert is not None
            or client_certificate_key is not None
        ):
            raise Exception("Miss configuration of client certificate.")
        return None
    return ClientCertificate(client_certificate_cert, client_certificate_key)


def get_driver(
    backend, uri=None, auth=None, client_certificate=None, **kwargs
):
    """Return default driver for tests that do not test this aspect."""
    if uri is None:
        scheme = get_neo4j_scheme()
        host, port = get_neo4j_host_and_port()
        uri = "%s://%s:%d" % (scheme, host, port)
    if auth is None:
        auth = get_authorization()
    if client_certificate is None:
        client_certificate = get_client_certificate()
    return Driver(
        backend, uri, auth, client_certificate=client_certificate, **kwargs
    )


class ServerInfo:
    def __init__(self, version: str, edition: str, cluster: bool, scheme: str):
        self.version = version
        self.edition = edition
        self.cluster = cluster
        self.scheme = scheme
        self._parsed_version = None
        self._is_dev_version = None

    @property
    def server_agent(self):
        if self.edition == "aura":
            raise ValueError(
                "We can't predict the server's agent string for aura!"
            )
        if self.is_dev_version:
            raise ValueError(
                "We can't predict the server's agent string for dev versions!"
            )
        return "Neo4j/" + self.version

    # [bolt-version-bump] search tag when updating IT matrix
    @property
    def max_protocol_version(self):
        if self.edition == "aura" and self.is_dev_version:
            return 6, 0
        version = self.parsed_version()
        if version >= (2026, 5):
            # TODO: adjust version when Bolt 6.1 goes GA
            # [uuid-preview] search tag for removal of UUID preview workarounds
            return 6, 1
        if version >= (2025, 10):
            return 6, 0
        if version >= (5, 26):
            # bolt 5.7 and 5.8 were released in a single server version
            return 5, 8
        if version >= (5, 23):
            return 5, 6
        # bolt 5.5 was never released
        if version >= (5, 13):
            return 5, 4
        if version >= (5, 9):
            return 5, 3
        if version >= (5, 7):
            return 5, 2
        if version >= (5, 5):
            return 5, 1
        if version >= (5, 0):
            return 5, 0
        if version >= (4, 4):
            return 4, 4
        if version >= (4, 3):
            return 4, 3
        if version >= (4, 2):
            return 4, 2
        raise ValueError(f"Unsupported Neo4j version to test: {self.version}")

    # [bolt-version-bump] search tag when updating IT matrix
    @property
    def max_query_api_version(self):
        if self.edition == "aura" and self.is_dev_version:
            return 1, 1
        version = self.parsed_version()
        if version >= (2025, 11):
            return 1, 1
        if version >= (5, 19):
            return 1, 0
        raise ValueError(f"Unsupported Neo4j version to test: {self.version}")

    def common_protocol_versions(self, driver_features):
        driver_bolt_features = bolt_versions_in_features(driver_features)
        max_server_protocol_version = self.max_protocol_version
        return [
            version
            for (version, _feature) in driver_bolt_features
            if version <= max_server_protocol_version
        ]

    def parsed_version(self):
        if self._parsed_version is None:
            self._parsed_version = parse_version(self.version)
        return self._parsed_version

    @property
    def is_dev_version(self):
        if self._is_dev_version is None:
            self._is_dev_version = bool(re.match(r"(\d+)\.dev", self.version))
        return self._is_dev_version

    @property
    def is_http(self):
        return self.scheme in {"http", "https"}

    @property
    def is_bolt(self):
        return bool(_BOLT_SCHEME_RE.match(self.scheme))

    @property
    def is_encrypted_protocol(self):
        return bool(re.match(r"^(?:bolt|neo4j)\+s(?:sc)?|https$", self.scheme))


def get_server_info():
    return ServerInfo(
        version=os.environ.get(env_neo4j_version, "5.0"),
        edition=os.environ.get(env_neo4j_edition, "enterprise"),
        cluster=(
            os.environ.get(env_neo4j_cluster, "False").lower()
            in ("true", "yes", "y", "1")
        ),
        scheme=get_neo4j_scheme(),
    )


def cluster_unsafe_test(func):
    def check(test_case):
        if get_server_info().cluster:
            test_case.skipTest("Test does not support cluster")

    return _make_skip_decorator(func, check)


def bolt_only_test(func):
    def check(test_case):
        if not get_server_info().is_bolt:
            test_case.skipTest(
                f"Test does not support {get_neo4j_scheme()} scheme"
            )

    return _make_skip_decorator(func, check)


def http_only_test(func):
    def check(test_case):
        if not get_server_info().is_http:
            test_case.skipTest(
                f"Test does not support {get_neo4j_scheme()} scheme"
            )

    return _make_skip_decorator(func, check)


def unencrypted_only_test(func):
    def check(test_case):
        if get_server_info().is_encrypted_protocol:
            test_case.skipTest(
                f"Test does not support encrypted scheme {get_neo4j_scheme()}"
            )

    return _make_skip_decorator(func, check)


def requires_multi_db_support(func):
    def check(test_case):
        if not has_multi_db_support(test_case):
            test_case.skipTest("Test requires support multiple databases.")

    return _make_skip_decorator(func, check)


def has_multi_db_support(test_case):
    server_info = get_server_info()
    return (
        has_min_bolt_version(test_case, (4, 0))
        and server_info.edition == "enterprise"
    )


def requires_vector_support(func):
    def check(test_case):
        if not has_vector_support(test_case):
            test_case.skipTest("Test requires support vector types.")

    return _make_skip_decorator(func, check)


def has_vector_support(test_case):
    server_info = get_server_info()
    protocol_support = has_min_protocol_version(
        test_case, bolt=(6, 0), http=(1, 1)
    )
    edition_support = server_info.edition in {"enterprise", "aura"}
    return protocol_support and edition_support


def requires_tx_support(func):
    def check(test_case):
        if not has_tx_support(test_case):
            test_case.skipTest("Test requires support transactions.")

    return _make_skip_decorator(func, check)


def has_tx_support(test_case):
    server_info = get_server_info()
    if server_info.is_bolt:
        return True
    elif server_info.is_http:
        return server_info.parsed_version() >= (5, 26)
    else:
        raise NotImplementedError(f"Unhandled scheme {server_info.scheme!r}")


def requires_tx_timeout_support(func):
    def check(test_case):
        if not has_tx_timeout_support(test_case):
            test_case.skipTest("Test requires support tx timeout.")

    return _make_skip_decorator(func, check)


def has_tx_timeout_support(test_case):
    return has_tx_support(test_case) and not get_server_info().is_http


def requires_tx_metadata_support(func):
    def check(test_case):
        if not has_tx_metadata_support(test_case):
            test_case.skipTest("Test requires support tx metadata.")

    return _make_skip_decorator(func, check)


def has_tx_metadata_support(test_case):
    return has_tx_support(test_case) and not get_server_info().is_http


def requires_summary_timers_support(func):
    def check(test_case):
        if not has_summary_timers_support(test_case):
            test_case.skipTest("Test requires support timers in summary.")

    return _make_skip_decorator(func, check)


def has_summary_timers_support(test_case):
    return not get_server_info().is_http


def requires_min_protocol_version(*, bolt, http):
    def check(test_case):
        require_min_protocol_version(test_case, bolt=bolt, http=http)

    def bolt_version_decorator(func):
        return _make_skip_decorator(func, check)

    return bolt_version_decorator


def require_min_protocol_version(test_case, *, bolt, http):
    if get_server_info().is_bolt:
        require_min_bolt_version(test_case, bolt)
    else:
        require_min_query_api_version(test_case, http)


def has_min_protocol_version(test_case, *, bolt, http):
    server_info = get_server_info()
    if server_info.is_bolt:
        return has_min_bolt_version(test_case, bolt)
    elif server_info.is_http:
        return has_min_query_api_version(test_case, http)
    else:
        raise NotImplementedError(f"Unhandled scheme {server_info.scheme!r}")


def requires_summary_query_type_support(func):
    def check(test_case):
        if not has_summary_query_type_support(test_case):
            test_case.skipTest("Test requires support query type in summary.")

    return _make_skip_decorator(func, check)


def has_summary_query_type_support(test_case):
    return not get_server_info().is_http


def has_utc_patch(test_case):
    server_info = get_server_info()
    if server_info.is_http:
        return Potential.YES
    if has_min_bolt_version(test_case, (5, 0)):
        return Potential.YES
    if has_min_bolt_version(test_case, (4, 3)):
        return Potential.MAYBE
    return Potential.NO


def requires_min_bolt_version(min_version):
    def check(test_case):
        require_min_bolt_version(test_case, min_version)

    def bolt_version_decorator(func):
        return _make_skip_decorator(func, check)

    return bolt_version_decorator


def require_min_bolt_version(test_case, min_version):
    if not isinstance(test_case, TestkitTestCase):
        raise TypeError("test_case should be a TestkitTestCase")
    reason = _skip_reason_min_bolt_version(test_case, min_version)
    if reason:
        test_case.skipTest(reason)


def has_min_bolt_version(test_case, min_version):
    if not isinstance(test_case, TestkitTestCase):
        raise TypeError("test_case should be a TestkitTestCase")
    return not _skip_reason_min_bolt_version(test_case, min_version)


def bolt_versions_in_features(features):
    return (
        (parse_version(f.value.split(":")[-1]), f)
        for f in features
        if re.match(r"^BOLT_(\d+_)*(\d+)$", f.name)
    )


def _skip_reason_min_bolt_version(test_case, min_version):
    if isinstance(min_version, str):
        min_version = parse_version(min_version)
    server_max_version = get_server_info().max_protocol_version
    all_version_features = bolt_versions_in_features(protocol.Feature)
    all_viable_versions = [
        feature
        for (version, feature) in all_version_features
        if min_version <= version <= server_max_version
    ]

    if server_max_version < min_version:
        return (
            "Server does not support minimum required "
            f"Bolt version: {min_version}"
        )
    missing = test_case.driver_missing_features(*all_viable_versions)
    if len(missing) == len(all_viable_versions):
        return (
            "There is no common version between server "
            "and driver that fulfills the minimum "
            f"required protocol version: {min_version}"
        )
    return None


def requires_min_query_api_version(min_version):
    def check(test_case):
        require_min_query_api_version(test_case, min_version)

    def query_api_version_decorator(func):
        return _make_skip_decorator(func, check)

    return query_api_version_decorator


def require_min_query_api_version(test_case, min_version):
    if not isinstance(test_case, TestkitTestCase):
        raise TypeError("test_case should be a TestkitTestCase")
    reason = _skip_reason_min_query_api_version(test_case, min_version)
    if reason:
        test_case.skipTest(reason)


def has_min_query_api_version(test_case, min_version):
    if not isinstance(test_case, TestkitTestCase):
        raise TypeError("test_case should be a TestkitTestCase")
    return not _skip_reason_min_query_api_version(test_case, min_version)


def query_api_versions_in_features(features):
    return (
        (parse_version(f.value.split(":")[-1]), f)
        for f in features
        if re.match(r"^HTTP_QUERY_API_(\d+_)*(\d+)$", f.name)
    )


def _skip_reason_min_query_api_version(test_case, min_version):
    if isinstance(min_version, str):
        min_version = parse_version(min_version)
    server_max_version = get_server_info().max_query_api_version
    all_version_features = query_api_versions_in_features(protocol.Feature)
    all_viable_versions = [
        feature
        for (version, feature) in all_version_features
        if min_version <= version <= server_max_version
    ]

    if server_max_version < min_version:
        return (
            "Server does not support minimum required "
            f"HTTP Query API version: {min_version}"
        )
    missing = test_case.driver_missing_features(*all_viable_versions)
    if len(missing) == len(all_viable_versions):
        return (
            "There is no common version between server "
            "and driver that fulfills the minimum "
            f"required protocol version: {min_version}"
        )
    return None


def parse_version(v: str):
    match = re.match(r"(\d+)\.dev", v)
    if match:
        return int(match.group(1)), float("inf")
    else:
        return tuple(int(i) for i in v.split(".")[:2])


def _make_skip_decorator(func, skip_hook):
    @wraps(func)
    def wrapper(*args, **kwargs):
        if len(args) >= 1 and isinstance(args[0], TestkitTestCase):
            skip_hook(args[0])
        else:
            raise TypeError(
                f"{func.__name__} must only decorate TestkitTestCase methods"
            )
        return func(*args, **kwargs)

    return wrapper


class QueryBuilder:
    @staticmethod
    def escape_identifier(identifier):
        identifier = identifier.replace("`", "``")
        return "`{}`".format(identifier)

    @staticmethod
    def _wait_clause(version):
        return " WAIT" if version >= (4, 2) else ""

    @staticmethod
    def create_db(database, if_not_exists=True, wait=True):
        version = get_server_info().parsed_version()
        return "CREATE DATABASE {}{}{}".format(
            QueryBuilder.escape_identifier(database),
            " IF NOT EXISTS" if if_not_exists else "",
            QueryBuilder._wait_clause(version) if wait else "",
        )

    @staticmethod
    def drop_db(database, if_exists=True, wait=True):
        version = get_server_info().parsed_version()
        return "DROP  DATABASE {}{}{}".format(
            QueryBuilder.escape_identifier(database),
            " IF EXISTS" if if_exists else "",
            QueryBuilder._wait_clause(version) if wait else "",
        )

    @staticmethod
    def call_subquery(subquery, imports=()):
        version = get_server_info().parsed_version()
        imports = ", ".join(list(map(QueryBuilder.escape_identifier, imports)))
        if not imports:
            return f"CALL {{\n    {subquery}\n}}"
        if version >= (5, 23):
            return f"CALL ({imports}) {{\n    {subquery}\n}}"
        else:
            return f"CALL {{\n    WITH {imports}\n    {subquery}\n}}"


def with_retries(work, *args, **kwargs):
    t0 = None
    t_last = time()
    while True:
        try:
            return work(*args, **kwargs)
        except protocol.DriverError as e:
            if not e.retryable:
                raise
            if t0 is None:
                t0 = time()
            if time() - t0 > 30:
                raise
            to_sleep = 0.5 - (time() - t_last)
            if to_sleep > 0:
                sleep(to_sleep)
            t_last = time()
            warn(
                f"Retrying due to retryable error: {traceback.format_exc()}",
                stacklevel=1,
            )
