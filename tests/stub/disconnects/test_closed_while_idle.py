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

    The connection has to die while it is sitting in the pool, so the script
    never runs to completion. Stopping the server is what closes it, and that
    happens after the session has been closed, so the driver has already
    released the connection by then. A pool of one forces the second query onto
    the same connection rather than leaving it to chance.
    """

    required_features = (
        types.Feature.BOLT_6_0,
        types.Feature.OPT_DEAD_CONNECTION_DETECTION,
    )

    def setUp(self):
        super().setUp()
        self._server = StubServer(9001)
        auth = types.AuthorizationToken("basic", principal="neo4j",
                                        credentials="pass")
        uri = f"bolt://{self._server.address}"
        self._driver = Driver(self._backend, uri, auth,
                              max_connection_pool_size=1)

    def tearDown(self):
        self._driver.close()
        self._server.reset()
        super().tearDown()

    def _run_query(self):
        with self._driver.session("r") as session:
            result = session.run("RETURN 1 as n")
            return list(result)

    def test_discards_connection_closed_while_idle(self):
        self._server.start(self.script_path("exit_while_idle.script"))

        first = self._run_query()
        self.assertEqual(len(first), 1)

        self._server.reset()
        self._server.start(self.script_path("exit_while_idle.script"))

        second = self._run_query()

        self.assertEqual(len(second), 1)
