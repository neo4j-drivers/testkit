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

    The transaction is deliberately left open. The server must not close the
    socket until the connection is back in the pool, and ending an open
    transaction is the one piece of cleanup no driver can skip, so it gives the
    script something to wait for. Releasing after a plain autocommit query
    leaves a driver with nothing to send, and the close then races the release.
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
            tx = session.begin_transaction()
            result = tx.run("RETURN 1 as n")
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

    def _start_server(self):
        # Ending the open transaction is the sync point: no driver can pool a
        # connection with a transaction still open, so every driver puts that
        # on the wire. A driver that also resets on release sends one more
        # message, which the script has to consume or the release blocks.
        if self.driver_supports_features(types.Feature.OPT_MINIMAL_RESETS):
            release_reset = ""
        else:
            release_reset = "C: RESET\nS: SUCCESS {}"

        self._server.start(self.script_path("exit_while_idle.script"),
                           vars_={"#RELEASE_RESET#": release_reset})

    def test_discards_connection_closed_while_idle(self):
        self._start_server()

        first = self._run_query()
        self.assertEqual(len(first), 1)

        closed = self._wait_for_server_to_close()
        self.assertTrue(closed, "server never closed the idle connection")

        second = self._run_query()

        self.assertEqual(len(second), 1)
        self.assertEqual(self._server.count_responses("<ACCEPT>"), 2)
