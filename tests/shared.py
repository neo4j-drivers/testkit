"""
Shared utilities for writing tests
Common between integration tests using Neo4j and stub server.

Uses environment variables for configuration:

TEST_BACKEND_HOST  Hostname of backend, default is localhost
TEST_BACKEND_PORT  Port on backend host, default is 9876
"""

import inspect
import os
import re
import socket
import unittest

import ifaddr

from nutkit import protocol
from nutkit.backend import Backend
import warnings


def get_backend_host_and_port():
    host = os.environ.get('TEST_BACKEND_HOST', '127.0.0.1')
    port = os.environ.get('TEST_BACKEND_PORT', 9876)
    return host, port


def new_backend():
    """ Returns connection to backend, caller is responsible for closing
    """
    host, port = get_backend_host_and_port()
    return Backend(host, port)


def get_ip_addresses(exclude_loopback=True):
    def pick_address(adapter_):
        ip6 = None
        for address_ in adapter_.ips:
            if address_.is_IPv4:
                return address_.ip
            elif ip6 is None:
                ip6 = address_.ip
        return ip6

    ips = []
    for adapter in ifaddr.get_adapters():
        if exclude_loopback:
            name = adapter.nice_name.lower()
            if name == "lo" or "loopback" in name:
                continue
        address = pick_address(adapter)
        if address:
            ips.append(address)

    return ips


def dns_resolve(host_name):
    _, _, ip_addresses = socket.gethostbyname_ex(host_name)
    return ip_addresses


def dns_resolve_single(host_name):
    ips = dns_resolve(host_name)
    if len(ips) != 1:
        raise ValueError("%s resolved to %i instead of 1 IP address"
                         % (host_name, len(ips)))
    return ips[0]


def get_dns_resolved_server_address(server):
    return "%s:%i" % (dns_resolve_single(server.host), server.port)


def driver_feature(*features):
    features = set(features)

    for feature in features:
        if not isinstance(feature, protocol.Feature):
            raise Exception('The arguments must be instances of Feature')

    def get_valid_test_case(*args, **kwargs):
        if not args or not isinstance(args[0], TestkitTestCase):
            raise Exception('Should only decorate TestkitTestCase methods')
        return args[0]

    def driver_feature_decorator(func):
        def wrapper(*args, **kwargs):
            test_case = get_valid_test_case(*args, **kwargs)
            test_case.skip_if_missing_driver_features(*features)
            return func(*args, **kwargs)
        return wrapper
    return driver_feature_decorator


class MemoizedSupplier:
    """ Momoize the function it annotates.
    This way the decorated function will always return the
    same value of the first interaction independent of the
    supplied params.
    """

    def __init__(self, func):
        self._func = func
        self._memo = None

    def __call__(self, *args, **kwargs):
        if self._memo is None:
            self._memo = self._func(*args, **kwargs)
        return self._memo


@MemoizedSupplier
def get_driver_features(backend):
    try:
        response = backend.sendAndReceive(protocol.GetFeatures())
        if not isinstance(response, protocol.FeatureList):
            raise Exception("Response is not instance of FeatureList")
        return set(response.features)
    except (OSError, protocol.BaseError) as e:
        warnings.warn("Could not fetch FeatureList: %s" % e)
        return set()


def get_driver_name():
    return os.environ['TEST_DRIVER_NAME']


class TestkitTestCase(unittest.TestCase):
    def setUp(self):
        super().setUp()
        id_ = re.sub(r"^([^\.]+\.)*?tests\.", "", self.id())
        self._backend = new_backend()
        self.addCleanup(self._backend.close)
        self._driver_features = get_driver_features(self._backend)
        response = self._backend.sendAndReceive(protocol.StartTest(id_))
        if isinstance(response, protocol.SkipTest):
            self.skipTest(response.reason)

        # TODO: remove this compatibility layer when all drivers are adapted
        if get_driver_name() in ("python", "java", "javascript",
                                 "go", "dotnet"):
            for exp, sub in (
                (r"^stub\.bookmarks\.test_bookmarks\.TestBookmarks",
                 "stub.bookmark.Tx"),
                (r"^stub\.disconnects\.test_disconnects\.TestDisconnects.",
                 "stub.disconnected.SessionRunDisconnected."),
                (r"^stub\.iteration\.[^.]+\.TestIterationSessionRun",
                 "stub.iteration.SessionRun"),
                (r"^stub\.iteration\.[^.]+\.TestIterationTxRun",
                 "stub.iteration.TxRun"),
                (r"^stub\.retry\.[^.]+\.", "stub.retry."),
                (r"^stub\.routing\.[^.]+\.", "stub.routing."),
                (r"^stub\.routing\.RoutingV4x1\.", "stub.routing.RoutingV4."),
                (r"^stub\.routing\.RoutingV4x3\.", "stub.routing.Routing."),
                (r"^stub\.session_run_parameters\."
                 r"[^.]+\.TestSessionRunParameters\.",
                 "stub.sessionparameters.SessionRunParameters."),
                (r"^stub\.tx_begin_parameters\.[^.]+\.TestTxBeginParameters\.",
                 "stub.txparameters.TxBeginParameters."),
                (r"^stub\.versions\.[^.]+\.TestProtocolVersions",
                 "stub.versions.ProtocolVersions"),
                (r"^stub\.transport\.[^.]+\.TestTransport\.",
                 "stub.transport.Transport."),
                (r"^stub\.authorization\.[^.]+\.TestAuthorizationV4x3\.",
                 "stub.authorization.AuthorizationTests."),
                (r"^stub\.authorization\.[^.]+\.TestAuthorizationV4x1\.",
                 "stub.authorization.AuthorizationTestsV4."),
                (r"^stub\.authorization\.[^.]+\.TestAuthorizationV3\.",
                 "stub.authorization.AuthorizationTestsV3."),
                (r"^stub\.authorization\.[^.]+\.TestNoRoutingAuthorization\.",
                 "stub.authorization.NoRoutingAuthorizationTests."),
                (r"^stub\.server_side_routing\.test_server_side_routing\."
                 r"TestServerSideRouting\.",
                 "stub.serversiderouting.ServerSideRouting."),
            ):
                id_ = re.sub(exp, sub, id_)
        response = self._backend.sendAndReceive(protocol.StartTest(id_))
        if isinstance(response, protocol.SkipTest):
            self.skipTest(response.reason)

        elif not isinstance(response, protocol.RunTest):
            raise Exception("Should be SkipTest or RunTest, "
                            "received {}: {}".format(type(response),
                                                     response))

    def driver_missing_features(self, *features):
        needed = set(map(lambda f: f.value, features))
        supported = self._driver_features
        return needed - supported

    def driver_supports_features(self, *features):
        return not self.driver_missing_features(*features)

    def skip_if_missing_driver_features(self, *features):
        missing = self.driver_missing_features(*features)
        if missing:
            self.skipTest("Needs support for %s" % ", ".join(missing))

    def script_path(self, *path):
        base_path = os.path.dirname(inspect.getfile(self.__class__))
        return os.path.join(base_path, "scripts", *path)
