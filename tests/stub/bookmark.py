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
