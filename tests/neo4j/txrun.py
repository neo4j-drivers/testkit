from tests.neo4j.shared import get_driver
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
        if get_driver_name() in ["java"]:
            self.skipTest("Rollback not implemented in backend")

        # Verifies that last bookmark is set on the session upon
        # succesful commit.
        self._session = self._driver.session("w")
        tx = self._session.beginTransaction()
        tx.run("CREATE (n:SessionNode) RETURN n")
        tx.rollback()
        bookmarks = self._session.lastBookmarks()
        self.assertEqual(len(bookmarks), 0)

    def test_does_not_update_last_bookmark_on_failure(self):
        self._session = self._driver.session("w")
        tx = self._session.beginTransaction()
        self.assertThrows(Exception, lambda: tx.run("RETURN").next())
        self.assertThrows(Exception, lambda: tx.commit())
        bookmarks = self._session.lastBookmarks()
        self.assertEqual(len(bookmarks), 0)

    def test_should_be_able_to_rollback_a_failure(self):
        self._session = self._driver.session("w")
        tx = self._session.beginTransaction()
        self.assertThrows(Exception, lambda: tx.run("RETURN").next())
        tx.rollback()

    def test_should_not_rollback_a_rollbacked_tx(self):
        self._session = self._driver.session("w")
        tx = self._session.beginTransaction()
        tx.run('CREATE (:TXNode1)').consume()
        tx.rollback()
        self.assertThrows(
            Exception,
            lambda: tx.rollback()
        )

    def test_should_not_commit_a_rollbacked_tx(self):
        self._session = self._driver.session("w")
        tx = self._session.beginTransaction()
        tx.run('CREATE (:TXNode1)').consume()
        tx.rollback()
        self.assertThrows(
            Exception,
            lambda: tx.commit()
        )

    def test_should_not_rollback_a_commited_tx(self):
        self._session = self._driver.session("w")
        tx = self._session.beginTransaction()
        tx.run('CREATE (:TXNode1)').consume()
        tx.commit()
        self.assertThrows(
            Exception,
            lambda: tx.rollback()
        )

    def test_should_not_commit_a_commited_tx(self):
        self._session = self._driver.session("w")
        tx = self._session.beginTransaction()
        tx.run('CREATE (:TXNode1)').consume()
        tx.commit()
        self.assertThrows(
            Exception,
            lambda: tx.commit()
        )

    def test_should_run_valid_query_in_invalid_tx(self):
        self._session = self._driver.session("w")
        tx = self._session.beginTransaction()
        self.assertThrows(
            Exception,
            lambda: tx.run("NOT CYPHER").consume()
        )
        self.assertThrows(
            Exception,
            lambda: tx.run("RETURN 42").next()
        )
        tx.rollback()

    def test_should_fail_run_in_a_commited_tx(self):
        self._session = self._driver.session("w")
        tx = self._session.beginTransaction()
        tx.commit()
        self.assertThrows(
            Exception,
            lambda: tx.run("RETURN 42").consume()
        )

    def test_should_fail_run_in_a_rollbacked_tx(self):
        self._session = self._driver.session("w")
        tx = self._session.beginTransaction()
        tx.rollback()
        self.assertThrows(
            Exception,
            lambda: tx.run("RETURN 42").consume()
        )

    def test_should_throws_exception_when_invalid_tx_params(self):
        self._session = self._driver.session("w")
        tx = self._session.beginTransaction()
        self.assertThrows(
            Exception,
            lambda: tx.run("RETURN $value", "invalid").next()
        )
        tx.rollback()

    def test_should_fail_to_run_query_for_unreacheable_bookmark(self):
        self._session = self._driver.session("w")
        tx1 = self._session.beginTransaction()
        result = tx1.run('CREATE ()')
        result.consume()
        tx1.commit()
        unreachableBookmark = self._session.lastBookmarks()[0] + "0"
        self._session.close()
        self._session = self._driver.session(
            "w",
            bookmarks=[unreachableBookmark]
        )
        tx2 = self._session.beginTransaction()
        self.assertThrows(
            Exception,
            lambda: tx2.run("CREATE ()").consume()
        )
        tx2.rollback()
