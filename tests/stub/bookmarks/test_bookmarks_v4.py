from nutkit.frontend import Driver
from nutkit.protocol import AuthorizationToken
from tests.shared import TestkitTestCase
from tests.stub.shared import StubServer


# Tests bookmarks from transaction
class TestBookmarksV4(TestkitTestCase):
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

    def test_bookmarks_can_be_set(self):
        def test():
            bookmarks = ["bm:%i" % (i + 1) for i in range(bm_count)]
            session = self._driver.session(mode[0], bookmarks=bookmarks)
            self.assertEqual(session.lastBookmarks(), bookmarks)
            session.close()

        for mode in ("read", "write"):
            # TODO: decide what we expect to happen when multiple bookmarks are
            #       passed in: return all or only the last one?
            for bm_count in (0, 1):
                with self.subTest(mode + "_%i_bookmarks" % bm_count):
                    test()

    # Tests that a committed transaction can return the last bookmark
    def test_last_bookmark(self):
        self._server.start(
            path=self.script_path(self.version_dir,
                                  "send_bookmark_write_tx.script")
        )
        session = self._driver.session("w")
        tx = session.beginTransaction()
        list(tx.run("RETURN 1 as n"))
        tx.commit()
        bookmarks = session.lastBookmarks()
        session.close()
        self._driver.close()
        self._server.done()

        self.assertEqual(bookmarks, ["bm"])

    def test_send_and_receive_bookmarks_read_tx(self):
        self._server.start(
            path=self.script_path(self.version_dir,
                                  "send_and_receive_bookmark_read_tx.script")
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
            path=self.script_path(self.version_dir,
                                  "send_and_receive_bookmark_write_tx.script"),
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

    def test_sequence_of_writing_and_reading_tx(self):
        self._server.start(
            path=
            self.script_path(self.version_dir,
                             "send_and_receive_bookmark_two_write_tx.script"))
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
            path=self.script_path(self.version_dir,
                                  "send_and_receive_bookmark_write_tx.script"),
            vars={
                "#BOOKMARKS#": '["neo4j:bookmark:v1:tx42", '
                               '"neo4j:bookmark:v1:tx43", '
                               '"neo4j:bookmark:v1:tx44", '
                               '"neo4j:bookmark:v1:tx45", '
                               '"neo4j:bookmark:v1:tx46"] '
            }
        )
        session = self._driver.session(
            accessMode="w",
            bookmarks=[
                "neo4j:bookmark:v1:tx42", "neo4j:bookmark:v1:tx43",
                "neo4j:bookmark:v1:tx44", "neo4j:bookmark:v1:tx45",
                "neo4j:bookmark:v1:tx46"
            ]
        )
        tx = session.beginTransaction()
        result = tx.run('MATCH (n) RETURN n.name AS name')
        result.next()
        tx.commit()
        bookmarks = session.lastBookmarks()

        self.assertEqual(bookmarks, ["neo4j:bookmark:v1:tx4242"])
        self._server.done()
