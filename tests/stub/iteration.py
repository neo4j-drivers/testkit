import unittest

from tests.shared import *
from tests.stub.shared import *
from nutkit.frontend import Driver, AuthorizationToken
import nutkit.protocol as types


script_pull_all = """
!: BOLT #VERSION#
!: AUTO RESET
!: AUTO HELLO
!: AUTO GOODBYE

C: RUN "RETURN 1 AS n" {} {}
   PULL { "n": -1 }
S: SUCCESS {"fields": ["n.name"]}
   RECORD ["1"]
   RECORD ["2"]
   RECORD ["3"]
   RECORD ["4"]
   RECORD ["5"]
   RECORD ["6"]
   SUCCESS {"type": "w"}
"""

script_pull_n = """
!: BOLT #VERSION#
!: AUTO RESET
!: AUTO HELLO
!: AUTO GOODBYE

C: RUN "RETURN 1 AS n" {} {}
   PULL { "n": 2 }
S: SUCCESS {"fields": ["n.name"]}
   RECORD ["1"]
   RECORD ["2"]
   SUCCESS {"has_more": true}
C: PULL { "n": 2 }
S: RECORD ["3"]
   RECORD ["4"]
   SUCCESS {"has_more": true}
C: PULL { "n": 2 }
#END#
"""

end_full_batch = """
S: RECORD ["5"]
S: RECORD ["6"]
S: SUCCESS {"type": "w"}
"""
end_half_batch = """
S: RECORD ["5"]
S: SUCCESS {"type": "w"}
"""
end_empty_batch = """
S: SUCCESS {"type": "w"}
"""
end_error = """
S: RECORD ["5"]
S: FAILURE {"code": "Neo.TransientError.Database.DatabaseUnavailable", "message": "<whatever>"}
"""

class IteratePullN(unittest.TestCase):
    def setUp(self):
        self._backend = new_backend()
        self._server = StubServer(9001)

    def tearDown(self):
        self._backend.close()
        self._server.reset()

    def _run(self, n, script, end, expectedSequence, expectedError=False):
        uri = "bolt://%s" % self._server.address
        driver = Driver(self._backend, uri, AuthorizationToken(scheme="basic"))
        self._server.start(script=script, vars={"#END#": end, "#VERSION#": "4"})
        session = driver.session("w", fetchSize=n)
        result = session.run("RETURN 1 AS n")
        gotError = False
        sequence = []
        while True:
            try:
                next = result.next()
            except Exception as e:
                gotError = True
                break
            if isinstance(next, types.NullRecord):
                break
            sequence.append(next.values[0].value)
        driver.close()
        self._server.done()
        self.assertEqual(expectedSequence, sequence)
        self.assertEqual(expectedError, gotError)


    # Last fetched batch is a full batch
    def test_full_batch(self):
        if get_driver_name() not in ['go', 'dotnet']:
            self.skipTest("Need support for specifying session fetch size in testkit backend")
        self._run(2, script_pull_n, end_full_batch, ["1", "2", "3", "4", "5", "6"])

    # Last fetched batch is half full (or more important not full)
    def test_half_batch(self):
        if get_driver_name() not in ['go', 'dotnet']:
            self.skipTest("Need support for specifying session fetch size in testkit backend")
        self._run(2, script_pull_n, end_half_batch, ["1", "2", "3", "4", "5"])

    # Last fetched batch is empty
    def test_empty_batch(self):
        if get_driver_name() not in ['go', 'dotnet']:
            self.skipTest("Need support for specifying session fetch size in testkit backend")
        self._run(2, script_pull_n, end_empty_batch, ["1", "2", "3", "4"])

    # Last batch returns an error
    def test_error(self):
        if get_driver_name() not in ['go', 'dotnet']:
            self.skipTest("Need support for specifying session fetch size in testkit backend")
        self._run(2, script_pull_n, end_error, ["1", "2", "3", "4", "5"], expectedError=True)

    # Support -1, not batched at all
    def test_all(self):
        if get_driver_name() not in ['go', 'dotnet']:
            self.skipTest("Need support for specifying session fetch size in testkit backend")
        self._run(-1, script_pull_all, "", ["1", "2", "3", "4", "5", "6"])

