from nutkit.frontend import Driver
from nutkit.protocol import AuthorizationToken
from tests.shared import TestkitTestCase
from tests.stub.shared import StubServer

# TODO: Tests for 3.5 (no support for PULL n)
script_commit = """
!: BOLT 4
!: AUTO HELLO
!: AUTO GOODBYE
!: AUTO RESET

C: BEGIN {}
S: SUCCESS {}
C: RUN "RETURN 1 as n" {} {}
   PULL {"n": 1000}
S: SUCCESS {"fields": ["n"]}
   SUCCESS {"type": "w"}
C: COMMIT
S: SUCCESS {"bookmark": "bm"}
"""

send_and_receive_bookmark_read_tx = """
!: BOLT 4
!: AUTO HELLO
!: AUTO RESET
!: AUTO GOODBYE

C: BEGIN {"bookmarks": ["neo4j:bookmark:v1:tx42"], "mode": "r"}
C: RUN "MATCH (n) RETURN n.name AS name" {} {}
   PULL {"n": 1000}
S: SUCCESS {}
   SUCCESS {"fields": ["name"]}
   SUCCESS {}
C: COMMIT
S: SUCCESS {"bookmark": "neo4j:bookmark:v1:tx4242"}

"""

send_and_receive_bookmark_write_tx = """
!: BOLT 4
!: AUTO HELLO
!: AUTO RESET
!: AUTO GOODBYE

C: BEGIN {"bookmarks": #BOOKMARKS# }
C: RUN "MATCH (n) RETURN n.name AS name" {} {}
   PULL {"n": 1000}
S: SUCCESS {}
   SUCCESS {"fields": ["name"]}
   SUCCESS {}
C: COMMIT
S: SUCCESS {"bookmark": "neo4j:bookmark:v1:tx4242"}

"""

sequecing_writing_and_reading_tx = """
!: BOLT 4
!: AUTO HELLO
!: AUTO RESET
!: AUTO GOODBYE

C: BEGIN {"bookmarks": ["neo4j:bookmark:v1:tx42"]}
C: RUN "MATCH (n) RETURN n.name AS name" {} {}
   PULL {"n": 1000}
S: SUCCESS {}
   SUCCESS {"fields": ["name"]}
   SUCCESS {}
C: COMMIT
S: SUCCESS {"bookmark": "neo4j:bookmark:v1:tx4242"}

C: BEGIN {"bookmarks": ["neo4j:bookmark:v1:tx4242"]}
C: RUN "MATCH (n) RETURN n.name AS name" {} {}
   PULL {"n": 1000}
S: SUCCESS {}
   SUCCESS {"fields": ["name"]}
   SUCCESS {}
C: COMMIT
S: SUCCESS {"bookmark": "neo4j:bookmark:v1:tx424242"}
"""

# Tests bookmarks from transaction
class Tx(TestkitTestCase):
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

        self._server.start(script=script_commit)
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
            script=send_and_receive_bookmark_read_tx
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
            script=send_and_receive_bookmark_write_tx,
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
        self._server.start(
            script=sequecing_writing_and_reading_tx
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

        txRead = session.beginTransaction()
        result = txRead.run('MATCH (n) RETURN n.name AS name')
        result.next()
        txRead.commit()

        bookmarks = session.lastBookmarks()
        self.assertEqual(bookmarks, ["neo4j:bookmark:v1:tx424242"])

        self._server.done()

    def test_send_and_receive_multiple_bookmarks_write_tx(self):
        self._server.start(
            script=send_and_receive_bookmark_write_tx,
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
