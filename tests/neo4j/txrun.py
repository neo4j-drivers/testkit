from tests.neo4j.shared import get_driver
from tests.shared import new_backend, get_driver_name, TestkitTestCase


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
