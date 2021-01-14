import unittest

from tests.shared import *
from tests.stub.shared import *
from nutkit.frontend import Driver, AuthorizationToken
import nutkit.protocol as types


class SessionRun(unittest.TestCase):
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
        self._run(2, SessionRun.script_pull_n, SessionRun.end_full_batch, ["1", "2", "3", "4", "5", "6"])

    # Last fetched batch is half full (or more important not full)
    def test_half_batch(self):
        self._run(2, SessionRun.script_pull_n, SessionRun.end_half_batch, ["1", "2", "3", "4", "5"])

    # Last fetched batch is empty
    def test_empty_batch(self):
        self._run(2, SessionRun.script_pull_n, SessionRun.end_empty_batch, ["1", "2", "3", "4"])

    # Last batch returns an error
    def test_error(self):
        self._run(2, SessionRun.script_pull_n, SessionRun.end_error, ["1", "2", "3", "4", "5"], expectedError=True)

    # Support -1, not batched at all
    def test_all(self):
        self._run(-1, SessionRun.script_pull_all, "", ["1", "2", "3", "4", "5", "6"])


class TxRun(unittest.TestCase):
    script_n = """
    !: BOLT #VERSION#
    !: AUTO HELLO
    !: AUTO GOODBYE
    !: AUTO RESET

    C: BEGIN {}
    S: SUCCESS {}
    C: RUN "CYPHER" {} {}
       PULL {"n": 2}
    S: SUCCESS {"fields": ["x"], "qid": 7}
       RECORD [1]
       RECORD [2]
       SUCCESS {"has_more": true}
    C: PULL {"n": 2, "qid": 7}
    S: RECORD [3]
       SUCCESS {"has_more": false}
    C: COMMIT
    S: SUCCESS {"bookmark": "bm"}
    """


    def setUp(self):
        self._backend = new_backend()
        self._server = StubServer(9001)

    def tearDown(self):
        self._backend.close()
        self._server.reset()

    def _iterate(self, n, script, expectedSequence, expectedError=False):
        uri = "bolt://%s" % self._server.address
        driver = Driver(self._backend, uri, AuthorizationToken(scheme="basic"))
        self._server.start(script=script, vars={"#VERSION#": "4"})
        session = driver.session("w", fetchSize=n)
        tx = session.beginTransaction()
        result = tx.run("CYPHER")
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
        tx.commit()
        driver.close()
        self._server.done()
        self.assertEqual(expectedSequence, sequence)
        self.assertEqual(expectedError, gotError)

    def test_all(self):
        self._iterate(2, TxRun.script_n, [1, 2, 3])

    script_nested_n = """
    !: BOLT #VERSION#
    !: AUTO HELLO
    !: AUTO GOODBYE
    !: AUTO RESET

    C: BEGIN {}
    S: SUCCESS {}
    C: RUN "CYPHER" {} {}
       PULL {"n": 1}
    S: SUCCESS {"fields": ["x"], "qid": 1}
       RECORD ["1_1"]
       SUCCESS {"has_more": true}
    #AFTER_ASK_RECORD_1_1#
    C: RUN "CYPHER" {} {}
       PULL {"n": 1}
    S: SUCCESS {"fields": ["x"], "qid": 2}
       RECORD ["2_1"]
       SUCCESS {"has_more": true}
    C: PULL {"n": 1, "qid": 2}
    S: RECORD ["2_2"]
       SUCCESS {"has_more": false}
    #AFTER_ASK_RECORD_2_2#
    C: RUN "CYPHER" {} {}
       PULL {"n": 1}
    S: SUCCESS {"fields": ["x"], "qid": 3}
       RECORD ["3_1"]
       SUCCESS {"has_more": false}
    C: COMMIT
    S: SUCCESS {"bookmark": "bm"}
    """

    script_record_1_2 = """
    C: PULL {"n": 1, "qid": 1}
    S: RECORD ["1_2"]
       SUCCESS {"has_more": false}
    """

    def test_nested(self):
        # ex JAVA - java completely pulls the first query before running the second
        if get_driver_name() not in ['go', 'dotnet', 'javascript']:
            self.skipTest("Need support for specifying session fetch size in testkit backend")
        uri = "bolt://%s" % self._server.address
        driver = Driver(self._backend, uri, AuthorizationToken(scheme="basic"))
        after_ask_record_1_1 = TxRun.script_record_1_2 if get_driver_name() in 'javascript' else ''
        after_ask_record_2_2 = TxRun.script_record_1_2 if get_driver_name() not in 'javascript' else ''
        self._server.start(script=TxRun.script_nested_n, vars={"#VERSION#": "4", "#AFTER_ASK_RECORD_1_1#": after_ask_record_1_1, "#AFTER_ASK_RECORD_2_2#": after_ask_record_2_2})
        session = driver.session("w", fetchSize=1)
        tx = session.beginTransaction()
        res1 = tx.run("CYPHER")
        seq = []
        seqs = []
        while True:
            rec1 = res1.next()
            if isinstance(rec1, types.NullRecord):
                break
            seq.append(rec1.values[0].value)
            seq2 = []
            res2 = tx.run("CYPHER")
            while True:
                rec2 = res2.next()
                if isinstance(rec2, types.NullRecord):
                    break
                seq2.append(rec2.values[0].value)
            seqs.append(seq2)

        tx.commit()
        driver.close()
        self._server.done()
        self.assertEqual(["1_1", "1_2"], seq)
        self.assertEqual([["2_1", "2_2"], ["3_1"]], seqs)

