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
            "#ADVERTISED_PORT#": server.port,
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
    def session(self, driver):
        session = driver.session("w")
        try:
            yield session
        finally:
            session.close()


class TestAdvertisedAddress(_AdvertisedAddressTestCase):
    required_features = (
        types.Feature.BOLT_5_8,
    )

    def _test_reuses_connection(
        self,
        server,
        *,
        driver_kwargs=None,
        repetitions=1,
    ):
        if driver_kwargs is None:
            driver_kwargs = {}

        dns_expectations = deque(
            (
                (
                    _FAKE_ADDRESS,
                    [server.host],
                ),
            )
        )

        def dns_resolver(name):
            nonlocal dns_expectations
            expectation, result = dns_expectations.popleft()
            name, sep, port = name.rpartition(":")
            assert name == expectation
            return [sep.join((host, port)) for host in result]

        with self.driver(
            server,
            dns_resolver=dns_resolver,
            **driver_kwargs
        ) as driver:
            for i in range(repetitions):
                with self.session(driver) as session:
                    list(session.run(f"RETURN {i + 1} AS n"))

    @driver_feature(types.Feature.BACKEND_DNS_RESOLVER)
    def test_reuses_connection_according_to_advertised_address_routing(self):
        with self.server("advertised_address_routing.script") as server:
            self._test_reuses_connection(
                server,
                driver_kwargs={"routing": True},
            )

    def test_reuses_connection_regardless_of_advertised_address_direct(self):
        with self.server("advertised_address_direct.script") as server:
            self._test_reuses_connection(
                server,
                driver_kwargs={
                    "routing": False,
                    "max_connection_pool_size": 1,
                },
                repetitions=2,
            )
