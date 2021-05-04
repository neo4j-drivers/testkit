import json

from nutkit.frontend import Driver
from nutkit.protocol import AuthorizationToken
from tests.shared import TestkitTestCase
from tests.stub.shared import StubServer


# TODO: Tests for 3.5 (no support for PULL n)

# Tests bookmarks from transaction
class TestBookmarks(TestkitTestCase):
    def setUp(self):
        super().setUp()
        self._server = StubServer(9001)
        uri = "bolt://%s" % self._server.address
        auth = AuthorizationToken(scheme="basic")
        self._driver = Driver(self._backend, uri, auth)

    def tearDown(self):
        # If test raised an exception this will make sure that the stub server
        # is killed and it's output is dumped for analysis.
        self._server.reset()
        super().tearDown()

    # Tests that a committed transaction can return the last bookmark
    def test_last_bookmark(self):
        self._server.start(
            path=self.script_path("send_bookmark_write_tx.script")
        )
        session = self._driver.session("w")
        tx = session.beginTransaction()
        tx.run("RETURN 1 as n")
        tx.commit()
        bookmarks = session.lastBookmarks()
        session.close()
        self._driver.close()
        self._server.done()

        self.assertEqual(bookmarks, ["bm"])

    def test_send_and_receive_bookmarks_read_tx(self):
        self._server.start(
            path=self.script_path("send_and_receive_bookmark_read_tx.script")
        )
        session = self._driver.session(
            accessMode="r",
            bookmarks=["neo4j:bookmark:v1:tx42"]
        )
        tx = session.beginTransaction()
        result = tx.run('MATCH (n) RETURN n.name AS name')
        result.next()
        tx.commit()
        bookmarks = session.lastBookmarks()

        self.assertEqual(bookmarks, ["neo4j:bookmark:v1:tx4242"])
        self._server.done()

    def test_send_and_receive_bookmarks_write_tx(self):
        self._server.start(
            path=self.script_path("send_and_receive_bookmark_write_tx.script"),
            vars={
                "#BOOKMARKS#": '["neo4j:bookmark:v1:tx42"]'
            }
        )
        session = self._driver.session(
            accessMode="w",
            bookmarks=["neo4j:bookmark:v1:tx42"]
        )
        tx = session.beginTransaction()
        result = tx.run('MATCH (n) RETURN n.name AS name')
        result.next()
        tx.commit()
        bookmarks = session.lastBookmarks()

        self.assertEqual(bookmarks, ["neo4j:bookmark:v1:tx4242"])
        self._server.done()

    def test_sequece_of_writing_and_reading_tx(self):
        self._server.start(path=self.script_path(
            "send_and_receive_bookmark_two_write_tx.script"
        ))
        session = self._driver.session(
            accessMode="w",
            bookmarks=["neo4j:bookmark:v1:tx42"]
        )
        tx = session.beginTransaction()
        result = tx.run('MATCH (n) RETURN n.name AS name')
        result.next()
        tx.commit()

        bookmarks = session.lastBookmarks()
        self.assertEqual(bookmarks, ["neo4j:bookmark:v1:tx4242"])

        txRead = session.beginTransaction()
        result = txRead.run('MATCH (n) RETURN n.name AS name')
        result.next()
        txRead.commit()

        bookmarks = session.lastBookmarks()
        self.assertEqual(bookmarks, ["neo4j:bookmark:v1:tx424242"])

        self._server.done()

    def test_send_and_receive_multiple_bookmarks_write_tx(self):
        self._server.start(
            path=self.script_path("send_and_receive_bookmark_write_tx.script"),
            vars={
                "#BOOKMARKS#":
                '["neo4j:bookmark:v1:tx42", "neo4j:bookmark:v1:tx43"]'
            }
        )
        session = self._driver.session(
            accessMode="w",
            bookmarks=[
                "neo4j:bookmark:v1:tx42",
                "neo4j:bookmark:v1:tx43"
            ]
        )
        tx = session.beginTransaction()
        result = tx.run('MATCH (n) RETURN n.name AS name')
        result.next()
        tx.commit()
        bookmarks = session.lastBookmarks()

        self.assertEqual(bookmarks, ["neo4j:bookmark:v1:tx4242"])
        self._server.done()
