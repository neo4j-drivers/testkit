from abc import ABC
from collections import deque
from contextlib import contextmanager

import nutkit.protocol as types
from nutkit.frontend import Driver
from tests.shared import TestkitTestCase
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
    def driver(self, server, routing=True, dns_resolver=None):
        auth = types.AuthorizationToken("bearer", credentials="foo")
        scheme = "neo4j" if routing else "bolt"
        uri = f"{scheme}://{_FAKE_ADDRESS}:{server.port}"
        driver = Driver(
            self._backend, uri, auth, domain_name_resolver_fn=dns_resolver,
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

    def test_advertised_address(self):
        with self.server("advertised_address.script") as server:

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

            with self.driver(server, dns_resolver=dns_resolver) as driver:
                with self.session(driver) as session:
                    list(session.run("RETURN 1 AS n"))

    # TODO: test direct connection should be re-used regardless
    #  of advertised address
