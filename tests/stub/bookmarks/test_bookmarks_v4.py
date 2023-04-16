import json
from contextlib import contextmanager

from nutkit import protocol as types
from nutkit.frontend import Driver
from nutkit.protocol import AuthorizationToken
from tests.shared import TestkitTestCase
from tests.stub.shared import StubServer


# Tests bookmarks from transaction
class TestBookmarksV4(TestkitTestCase):

    required_features = types.Feature.BOLT_4_4,

    version_dir = "v4"

    def setUp(self):
        super().setUp()
        self._server = StubServer(9001)
        uri = "bolt://%s" % self._server.address
        auth = AuthorizationToken("basic", principal="", credentials="")
        self._driver = Driver(self._backend, uri, auth)

    def tearDown(self):
        # If test raised an exception this will make sure that the stub server
        # is killed and it's output is dumped for analysis.
        self._server.reset()
        super().tearDown()

    @contextmanager
    def _new_server(self, tx, bms_in, bm_out):
        script = "bookmarks_tx.script" if tx else "bookmarks.script"
        vars_ = {}
        if bms_in:
            vars_["#BM_IN#"] = '"bookmarks{}": %s' % json.dumps(bms_in)
        else:
            vars_["#BM_IN#"] = '"[bookmarks]": []'
        assert bm_out
        vars_["#BM_OUT#"] = str(bm_out)
        self._server.start(
            path=self.script_path(self.version_dir, script),
            vars_=vars_
        )
        try:
            yield self._server
        finally:
            self._server.reset()

    @contextmanager
    def _new_driver(self):
        if self._driver:
            self._driver.close()
        uri = "bolt://%s" % self._server.address
        auth = AuthorizationToken("basic", principal="", credentials="")
        self._driver = Driver(self._backend, uri, auth)
        try:
            yield self._driver
        finally:
            self._driver.close()
            self._driver = None

    def test_bookmarks_on_unused_sessions_are_returned(self):
        def test(mode_, bm_count_):
            bookmarks = ["bm:%i" % (i + 1) for i in range(bm_count_)]
            session = self._driver.session(mode_[0], bookmarks=bookmarks)
            self.assertEqual(sorted(session.last_bookmarks()),
                             sorted(bookmarks))
            session.close()

        for mode in ("read", "write"):
            for bm_count in (0, 1, 2):
                with self.subTest(mode=mode, bm_count=bm_count):
                    test(mode, bm_count)

    def test_bookmarks_session_run(self):
        def test(mode_, bm_count_, check_bms_pre_query_, consume_):
            bookmarks = ["bm:%i" % (i + 1) for i in range(bm_count_)]
            with self._new_server(tx=False, bms_in=bookmarks,
                                  bm_out="bm:re") as server:
                with self._new_driver() as driver:
                    session = driver.session(mode_[0], bookmarks=bookmarks)
                    if check_bms_pre_query_:
                        self.assertEqual(sorted(session.last_bookmarks()),
                                         sorted(bookmarks))
                    res = session.run("RETURN 1 AS n")
                    if consume_:
                        res.consume()
                    self.assertEqual(session.last_bookmarks(), ["bm:re"])
                    session.close()
                server.done()

        for mode in ("read", "write"):
            for bm_count in (0, 1, 2):
                for check_bms_pre_query in (False, True):
                    # TODO: make a decision if consume should be triggered
                    #       implicitly or not.
                    for consume in (False, True)[1:]:
                        with self.subTest(
                            mode=mode, bm_count=bm_count,
                            check_bms_pre_query=check_bms_pre_query,
                            consume=consume
                        ):
                            test(mode, bm_count, check_bms_pre_query, consume)

    def test_bookmarks_tx_run(self):
        def test(mode_, bm_count_, check_bms_pre_query_, consume_):
            bookmarks = ["bm:%i" % (i + 1) for i in range(bm_count_)]
            with self._new_server(tx=True, bms_in=bookmarks,
                                  bm_out="bm:re") as server:
                with self._new_driver() as driver:
                    session = driver.session(mode_[0], bookmarks=bookmarks)
                    if check_bms_pre_query_:
                        self.assertEqual(sorted(session.last_bookmarks()),
                                         sorted(bookmarks))
                    tx = session.begin_transaction()
                    res = tx.run("RETURN 1 AS n")
                    if consume_:
                        res.consume()
                    if check_bms_pre_query_:
                        self.assertEqual(sorted(session.last_bookmarks()),
                                         sorted(bookmarks))
                    tx.commit()
                    self.assertEqual(session.last_bookmarks(), ["bm:re"])
                    session.close()
                server.done()

        for mode in ("read", "write"):
            for bm_count in (0, 1, 2):
                for check_bms_pre_query in (False, True):
                    for consume in (False, True):
                        with self.subTest(
                            mode=mode, bm_count=bm_count,
                            check_bms_pre_query=check_bms_pre_query,
                            consume=consume
                        ):
                            test(mode, bm_count, check_bms_pre_query, consume)

    def test_bookmarks_tx_func(self):
        def work_consume(tx):
            res = tx.run("RETURN 1 AS n")
            res.consume()

        def work_no_consume(tx):
            tx.run("RETURN 1 AS n")

        def test(mode_, bm_count_, check_bms_pre_query_, consume_):
            bookmarks = ["bm:%i" % (i + 1) for i in range(bm_count_)]
            with self._new_server(tx=True, bms_in=bookmarks,
                                  bm_out="bm:re") as server:
                with self._new_driver() as driver:
                    session = driver.session(mode_[0], bookmarks=bookmarks)
                    if check_bms_pre_query_:
                        self.assertEqual(sorted(session.last_bookmarks()),
                                         sorted(bookmarks))
                    work = work_consume if consume_ else work_no_consume
                    if mode == "write":
                        session.execute_write(work)
                    else:
                        session.execute_read(work)
                    self.assertEqual(session.last_bookmarks(), ["bm:re"])
                    session.close()
                server.done()

        for mode in ("read", "write"):
            for bm_count in (0, 1, 2):
                for check_bms_pre_query in (False, True):
                    for consume in (False, True):
                        with self.subTest(
                            mode=mode, bm_count=bm_count,
                            check_bms_pre_query=check_bms_pre_query,
                            consume=consume
                        ):
                            test(mode, bm_count, check_bms_pre_query, consume)

    def test_sequence_of_writing_and_reading_tx(self):
        self._server.start(
            path=self.script_path(
                self.version_dir,
                "send_and_receive_bookmark_two_write_tx.script"
            )
        )
        session = self._driver.session(
            access_mode="w",
            bookmarks=["neo4j:bookmark:v1:tx42"]
        )
        tx = session.begin_transaction()
        result = tx.run("MATCH (n) RETURN n.name AS name")
        result.next()
        tx.commit()

        bookmarks = session.last_bookmarks()
        self.assertEqual(bookmarks, ["neo4j:bookmark:v1:tx4242"])

        tx_read = session.begin_transaction()
        result = tx_read.run("MATCH (n) RETURN n.name AS name")
        result.next()
        tx_read.commit()

        bookmarks = session.last_bookmarks()
        self.assertEqual(bookmarks, ["neo4j:bookmark:v1:tx424242"])

        self._server.done()
