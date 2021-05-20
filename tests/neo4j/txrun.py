from tests.neo4j.shared import get_driver
import nutkit.protocol as types
from tests.shared import (
    get_driver_name,
    TestkitTestCase,
)


class TestTxRun(TestkitTestCase):
    def setUp(self):
        super().setUp()
        self._driver = get_driver(self._backend)
        self._session = None

    def tearDown(self):
        if self._session:
            self._session.close()
        self._driver.close()
        super().tearDown()

    def test_updates_last_bookmark_on_commit(self):
        # Verifies that last bookmark is set on the session upon
        # succesful commit.
        self._session = self._driver.session("w")
        tx = self._session.beginTransaction()
        tx.run("CREATE (n:SessionNode) RETURN n")
        tx.commit()
        bookmarks = self._session.lastBookmarks()
        self.assertEqual(len(bookmarks), 1)
        self.assertGreater(len(bookmarks[0]), 3)

    def test_does_not_update_last_bookmark_on_rollback(self):
        # Verifies that last bookmark is set on the session upon
        # succesful commit.
        self._session = self._driver.session("w")
        tx = self._session.beginTransaction()
        tx.run("CREATE (n:SessionNode) RETURN n")
        tx.rollback()
        bookmarks = self._session.lastBookmarks()
        self.assertEqual(len(bookmarks), 0)

    def test_does_not_update_last_bookmark_on_failure(self):
        session = self._driver.session("w")
        tx = session.beginTransaction()
        with self.assertRaises(types.responses.DriverError):
            tx.run("RETURN").next()
            tx.commit()
        bookmarks = session.lastBookmarks()
        self.assertEqual(len(bookmarks), 0)

    def test_should_be_able_to_rollback_a_failure(self):
        if get_driver_name() in ["go"]:
            self.skipTest('Could not rollback transaction')
        session = self._driver.session("w")
        tx = session.beginTransaction()
        with self.assertRaises(types.responses.DriverError):
            tx.run("RETURN").next()
        tx.rollback()

    def test_should_not_rollback_a_rollbacked_tx(self):
        if get_driver_name() in ["go"]:
            self.skipTest('Does not raise the exception')
        self._session = self._driver.session("w")
        tx = self._session.beginTransaction()
        tx.run('CREATE (:TXNode1)').consume()
        tx.rollback()
        with self.assertRaises(types.responses.DriverError):
            tx.rollback()

    def test_should_not_rollback_a_commited_tx(self):
        if get_driver_name() in ["go"]:
            self.skipTest('Does not raise the exception')
        session = self._driver.session("w")
        tx = session.beginTransaction()
        tx.run('CREATE (:TXNode1)').consume()
        tx.commit()
        with self.assertRaises(types.responses.DriverError):
            tx.rollback()

    def test_should_not_commit_a_commited_tx(self):
        if get_driver_name() in ["go"]:
            self.skipTest('Does not raise exception')
        self._session = self._driver.session("w")
        tx = self._session.beginTransaction()
        tx.run('CREATE (:TXNode1)').consume()
        tx.commit()
        with self.assertRaises(types.responses.DriverError):
            tx.commit()

    def test_should_not_run_valid_query_in_invalid_tx(self):
        if get_driver_name() in ["python"]:
            self.skipTest("executes the second RUN")
        if get_driver_name() in ["go"]:
            self.skipTest('Could not rollback transaction')

        session = self._driver.session("w")
        tx = session.beginTransaction()
        with self.assertRaises(types.responses.DriverError):
            tx.run("NOT CYPHER").consume()

        with self.assertRaises(types.responses.DriverError):
            tx.run("RETURN 42").next()

        tx.rollback()

    def test_should_fail_run_in_a_commited_tx(self):
        self._session = self._driver.session("w")
        tx = self._session.beginTransaction()
        tx.commit()
        with self.assertRaises(types.responses.DriverError):
            tx.run("RETURN 42").next()

    def test_should_fail_run_in_a_rollbacked_tx(self):
        self._session = self._driver.session("w")
        tx = self._session.beginTransaction()
        tx.rollback()
        with self.assertRaises(types.responses.DriverError):
            tx.run("RETURN 42").next()

    def test_should_fail_to_run_query_for_unreacheable_bookmark(self):
        if get_driver_name() in ["go", "python"]:
            self.skipTest("Does not throws exception on run unreachable bm")
        session = self._driver.session(accessMode="w")
        tx1 = session.beginTransaction()
        result = tx1.run('CREATE ()')
        result.consume()
        tx1.commit()
        lastBookmark = session.lastBookmarks()[0]
        unreachableBookmark = lastBookmark[:-1] + "0"
        session.close()
        session = self._driver.session(
            accessMode="w",
            bookmarks=[unreachableBookmark]
        )

        with self.assertRaises(types.responses.DriverError):
            tx2 = session.beginTransaction()
            tx2.run("CREATE ()").consume()
