import time

import nutkit.protocol as types
from nutkit.frontend import Driver
from tests.shared import TestkitTestCase
from tests.stub.shared import StubServer


class TestClosedWhileIdle(TestkitTestCase):
    """The server drops an idle pooled connection and the driver copes.

    This is the Aura roll case: the instance goes away and takes every idle
    connection with it, without the driver being told. A driver that only
    tracks its own view of the socket will hand the dead connection straight to
    the next query.
    """

    required_features = (
        types.Feature.BOLT_5_4,
        types.Feature.OPT_DEAD_CONNECTION_DETECTION,
    )

    def setUp(self):
        super().setUp()
        self._server = StubServer(9001)
        auth = types.AuthorizationToken("basic", principal="neo4j",
                                        credentials="pass")
        uri = f"bolt://{self._server.address}"
        self._driver = Driver(self._backend, uri, auth)

    def tearDown(self):
        self._driver.close()
        self._server.reset()
        super().tearDown()

    def _run_query(self):
        session = self._driver.session("r")
        try:
            result = session.run("RETURN 1 as n")
            return result.list()
        finally:
            session.close()

    def _wait_for_server_to_close(self, timeout=10):
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self._server.count_responses("<EXIT>") >= 1:
                return True
            time.sleep(0.1)
        return False

    def test_discards_connection_closed_while_idle(self):
        self._server.start(self.script_path("exit_while_idle.script"))

        first = self._run_query()
        self.assertEqual(len(first), 1)

        closed = self._wait_for_server_to_close()
        self.assertTrue(closed, "server never closed the idle connection")

        second = self._run_query()

        self.assertEqual(len(second), 1)
        self.assertEqual(self._server.count_responses("<ACCEPT>"), 2)
