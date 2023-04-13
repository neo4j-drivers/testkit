"""
Shared utilities for writing tests.

Common between integration tests using Neo4j and stub server.

Uses environment variables for configuration:

TEST_BACKEND_HOST  Hostname of backend, default is localhost
TEST_BACKEND_PORT  Port on backend host, default is 9876
"""


from contextlib import contextmanager
import enum
import functools
import inspect
import os
import re
import socket
import unittest
import warnings

import ifaddr

from nutkit import protocol
from nutkit.backend import Backend


def get_backend_host_and_port():
    host = os.environ.get("TEST_BACKEND_HOST", "127.0.0.1")
    port = int(os.environ.get("TEST_BACKEND_PORT", 9876))
    return host, port


def new_backend():
    """Return connection to backend, caller is responsible for closing."""
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
            raise Exception("The arguments must be instances of Feature")

    def get_valid_test_case(*args, **kwargs):
        if not args or not isinstance(args[0], TestkitTestCase):
            raise Exception("Should only decorate TestkitTestCase methods")
        return args[0]

    def driver_feature_decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            test_case = get_valid_test_case(*args, **kwargs)
            test_case.skip_if_missing_driver_features(*features)
            return func(*args, **kwargs)
        return wrapper
    return driver_feature_decorator


class MemoizedSupplier:
    """Memoize the function it annotates.

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
        response = backend.send_and_receive(protocol.GetFeatures())
        if not isinstance(response, protocol.FeatureList):
            raise Exception("Response is not instance of FeatureList")
        raw_features = set(response.features)
        features = set()
        for raw in raw_features:
            features.add(protocol.Feature(raw))
        # TODO: remove this block once all drivers list this feature
        #       they all support the functionality already
        if get_driver_name() in ["go"]:
            assert protocol.Feature.API_SSL_SCHEMES not in features
            features.add(protocol.Feature.API_SSL_SCHEMES)
        print("features", features)
        return features
    except (OSError, protocol.BaseError) as e:
        warnings.warn(f"Could not fetch FeatureList: {e}")  # noqa: B028
        return set()


def get_driver_name():
    return os.environ["TEST_DRIVER_NAME"]


class TestkitTestCase(unittest.TestCase):

    required_features = None

    def setUp(self):
        super().setUp()
        self._testkit_test_name = id_ = re.sub(
            r"^([^\.]+\.)*?tests\.", "", self.id()
        )
        self._check_subtests = False
        self._backend = new_backend()
        self.addCleanup(self._backend.close)
        self._driver_features = get_driver_features(self._backend)

        if self.required_features:
            self.skip_if_missing_driver_features(*self.required_features)

        response = self._backend.send_and_receive(protocol.StartTest(id_))
        if isinstance(response, protocol.SkipTest):
            self.skipTest(response.reason)
        elif isinstance(response, protocol.RunSubTests):
            self._check_subtests = True
        elif not isinstance(response, protocol.RunTest):
            raise Exception("Should be SkipTest, RunSubTests, or RunTest, "
                            "received {}: {}".format(type(response),
                                                     response))

    def driver_missing_features(self, *features):
        needed = set(features)
        supported = self._driver_features
        return needed - supported

    def driver_supports_features(self, *features):
        return not self.driver_missing_features(*features)

    def _bolt_version_to_feature(self, version):
        if isinstance(version, protocol.Feature):
            return self.driver_supports_features(version)
        elif isinstance(version, str):
            m = re.match(r"\D*(\d+)(?:\D+(\d+))?", version)
            if not m:
                raise ValueError("Invalid bolt version specification")
            version = tuple(map(int, m.groups("0")))
        else:
            version = tuple(version)
        version = tuple(map(str, version))
        if len(version) == 1:
            version = (version[0], "0")
        try:
            return getattr(protocol.Feature, "_".join(("BOLT", *version)))
        except AttributeError:
            raise ValueError("Unknown bolt feature "
                             + "_".join(("BOLT", *version)))

    def driver_supports_bolt(self, version):
        return self.driver_supports_features(
            self._bolt_version_to_feature(version)
        )

    def skip_if_missing_driver_features(self, *features):
        missing = self.driver_missing_features(*features)
        if missing:
            self.skipTest("Needs support for %s" % ", ".join(
                map(str, missing)
            ))

    def skip_if_missing_bolt_support(self, version):
        self.skip_if_missing_driver_features(
            self._bolt_version_to_feature(version)
        )

    def script_path(self, *path):
        base_path = os.path.dirname(inspect.getfile(self.__class__))
        return os.path.join(base_path, "scripts", *path)

    @contextmanager
    def subTest(self, **params):  # noqa: N802
        assert "msg" not in params
        subtest_context = super().subTest(**params)
        with subtest_context:
            if not self._check_subtests:
                yield
                return
            try:
                response = self._backend.send_and_receive(
                    protocol.StartSubTest(self._testkit_test_name, params)
                )
            except Exception as outer_exc:
                try:
                    yield
                finally:
                    raise outer_exc
            # we have to run the subtest, but we don't care for the result
            # if we want to throw or skip (in fact also a throw)
            try:
                yield
            finally:
                if isinstance(response, protocol.SkipTest):
                    # skipping after the fact :/
                    self.skipTest(response.reason)
                elif not isinstance(response, protocol.RunTest):
                    raise Exception("Should be SkipTest, or RunTest, "
                                    "received {}: {}".format(type(response),
                                                             response))


class Potential(enum.Enum):
    YES = 1.0
    NO = 0.0
    MAYBE = 0.5
    # CAN_YOU_REPEAT_THE_QUESTION = "?"
