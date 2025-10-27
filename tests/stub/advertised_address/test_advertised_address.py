from abc import ABC
from collections import deque
from contextlib import contextmanager

import nutkit.protocol as types
from nutkit.frontend import Driver
from tests.shared import (
    driver_feature,
    TestkitTestCase,
)
from tests.stub.shared import StubServer

_FAKE_ADDRESS = "banana.example.com"
_FAKE_ADVERTISED_ADDRESS = "cucumber.example.com"


class _AdvertisedAddressTestCase(TestkitTestCase, ABC):
    @contextmanager
    def server(self, script, vars_=None):
        server = StubServer(9001)
        vars_ = {
            "#ADVERTISED_HOST#": f"{_FAKE_ADVERTISED_ADDRESS}",
            "#PORT#": server.port,
            "#HOST#": server.host,
            **(vars_ or {}),
        }
        server.start(path=self.script_path(script),
                     vars_=vars_)
        try:
            yield server
        except Exception:
            server.reset()
            raise

        server.done()

    @contextmanager
    def driver(self, server, routing=True, dns_resolver=None, **kwargs):
        auth = types.AuthorizationToken("bearer", credentials="foo")
        scheme = "neo4j" if routing else "bolt"
        uri = f"{scheme}://{_FAKE_ADDRESS}:{server.port}"
        driver = Driver(
            self._backend, uri, auth, domain_name_resolver_fn=dns_resolver,
            **kwargs,
        )
        try:
            yield driver
        finally:
            driver.close()

    @contextmanager
    def session(self, driver, access_mode="w", session_config=None):
        if session_config is None:
            session_config = {}
        session = driver.session(access_mode, **session_config)
        try:
            yield session
        finally:
            session.close()

    @staticmethod
    def _make_dns_resolver(*expected_resolved_pairs):
        dns_expectations = deque(expected_resolved_pairs)

        def dns_resolver(name):
            nonlocal dns_expectations
            expectation, result = dns_expectations.popleft()
            parts = name.rsplit(":", 1)
            sep = port = ""
            if len(parts) == 2:
                name, port = parts
                sep = ":"
            print(parts, expectation)
            assert name == expectation
            return [sep.join((host, port)) for host in result]

        return dns_resolver


class TestAdvertisedAddress(_AdvertisedAddressTestCase):
    required_features = (
        types.Feature.BOLT_5_8,
    )

    def test_routing_driver_reuses_connection_according_to_advertised_address(
        self,
    ):
        with self.server("advertised_address_routing.script") as server:
            self._test_reuses_connection(
                server,
                driver_kwargs={"routing": True},
            )

    def test_direct_driver_reuses_connection_regardless_of_advertised_address(
        self
    ):
        with self.server("advertised_address_direct.script") as server:
            self._test_reuses_connection(
                server,
                driver_kwargs={
                    "routing": False,
                    "max_connection_pool_size": 1,
                },
                repetitions=2,
            )

    @driver_feature(types.Feature.BACKEND_DNS_RESOLVER)
    def _test_reuses_connection(
        self,
        server,
        *,
        driver_kwargs=None,
        repetitions=1,
    ):
        if driver_kwargs is None:
            driver_kwargs = {}

        dns_resolver = self._make_dns_resolver((_FAKE_ADDRESS, [server.host]))

        with self.driver(
            server,
            dns_resolver=dns_resolver,
            **driver_kwargs
        ) as driver:
            for i in range(repetitions):
                with self.session(driver) as session:
                    list(session.run(f"RETURN {i + 1} AS n"))

    @driver_feature(
        types.Feature.BACKEND_DNS_RESOLVER,
        types.Feature.API_SESSION_AUTH_CONFIG,
    )
    def test_warm_routing_driver_reuses_connection_according_to_advertised_address(  # noqa: E501
        self,
    ):
        with self.server("advertised_address_routing_warm.script") as server:
            self._test_reuses_warm_connection(
                server,
                driver_kwargs={
                    "routing": True,
                    "max_connection_pool_size": 1,
                }
            )

    @driver_feature(
        types.Feature.BACKEND_DNS_RESOLVER,
        types.Feature.API_SESSION_AUTH_CONFIG,
    )
    def test_warm_direct_driver_reuses_connection_according_to_advertised_address(  # noqa: E501
        self,
    ):
        with self.server("advertised_address_direct_warm.script") as server:
            self._test_reuses_warm_connection(
                server,
                driver_kwargs={
                    "routing": False,
                    "max_connection_pool_size": 1,
                }
            )

    @driver_feature(types.Feature.BACKEND_DNS_RESOLVER)
    def _test_reuses_warm_connection(
        self,
        server,
        *,
        driver_kwargs=None,
    ):
        if driver_kwargs is None:
            driver_kwargs = {}

        dns_resolver = self._make_dns_resolver((_FAKE_ADDRESS, [server.host]))

        with self.driver(
            server,
            dns_resolver=dns_resolver,
            **driver_kwargs
        ) as driver:
            with self.session(
                driver,
                session_config={"database": "neo4j"},
            ) as session:
                list(session.run("RETURN 1 AS n"))

            # Using session auth to cause LOGOFF/LOGON
            # during which the server will change its advertised address.
            auth = types.AuthorizationToken("bearer", credentials="bar")
            with self.session(
                driver,
                session_config={"database": "neo4j", "auth_token": auth}
            ) as session:
                list(session.run("RETURN 2 AS n"))

            with self.session(
                driver,
                access_mode="r",
                session_config={"database": "neo4j"},
            ) as session:
                list(session.run("RETURN 3 AS n"))
