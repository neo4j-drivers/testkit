from nutkit.frontend import Driver
import nutkit.protocol as types
from tests.shared import (
    driver_feature,
    TestkitTestCase,
)
from tests.stub.shared import StubServer


class TestIterationTxRun(TestkitTestCase):

    def setUp(self):
        super().setUp()
        self._server = StubServer(9001)

    def tearDown(self):
        self._server.reset()
        super().tearDown()

    def _iterate(self, n, script_fn, expected_sequence, expected_error=False,
                 protocol_version="v4x4"):
        uri = "bolt://%s" % self._server.address
        driver = Driver(self._backend, uri,
                        types.AuthorizationToken("basic", principal="",
                                                 credentials=""))
        self._server.start(path=self.script_path(protocol_version, script_fn))
        session = driver.session("w", fetch_size=n)
        tx = session.begin_transaction()
        result = tx.run("CYPHER")
        got_error = False
        sequence = []
        while True:
            try:
                next_ = result.next()
            except types.DriverError:
                got_error = True
                break
            if isinstance(next_, types.NullRecord):
                break
            sequence.append(next_.values[0].value)
        tx.commit()
        driver.close()
        self._server.done()
        self.assertEqual(expected_sequence, sequence)
        self.assertEqual(expected_error, got_error)

    @driver_feature(types.Feature.BOLT_4_4)
    def test_batch(self):
        self._iterate(2, "tx_pull_2.script", [1, 2, 3])

    @driver_feature(types.Feature.BOLT_4_4)
    def test_all(self):
        self._iterate(-1, "tx_pull_all.script", [1, 2, 3])

    @driver_feature(types.Feature.BOLT_4_4)
    def test_all_slow_connection(self):
        self._iterate(-1, "tx_pull_all_slow_connection.script", [1, 2, 3])

    @driver_feature(types.Feature.BOLT_3_0)
    def test_batch_v3(self):
        # there is no incremental pulling for BOLTv3
        self._iterate(2, "tx_pull_all.script", [1, 2, 3],
                      protocol_version="v3")

    @driver_feature(types.Feature.BOLT_3_0)
    def test_all_v3(self):
        self._iterate(-1, "tx_pull_all.script", [1, 2, 3],
                      protocol_version="v3")

    @driver_feature(types.Feature.BOLT_4_4)
    def test_nested(self):
        uri = "bolt://%s" % self._server.address
        driver = Driver(self._backend, uri,
                        types.AuthorizationToken("basic", principal="",
                                                 credentials=""))
        self._server.start(path=self.script_path("v4x4",
                                                 "tx_pull_1_nested.script"))
        session = driver.session("w", fetch_size=1)
        tx = session.begin_transaction()
        res1 = tx.run("CYPHER")
        seq = []
        seqs = []
        while True:
            rec1 = res1.next()
            if isinstance(rec1, types.NullRecord):
                break
            seq.append(rec1.values[0].value)
            seq2 = []
            res2 = tx.run("CYPHER NESTED")
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

    @driver_feature(types.Feature.BOLT_4_4, types.Feature.API_RESULT_LIST,
                    types.Feature.OPT_RESULT_LIST_FETCH_ALL)
    def test_nested_using_list(self):
        uri = "bolt://%s" % self._server.address
        driver = Driver(self._backend, uri,
                        types.AuthorizationToken("basic", principal="",
                                                 credentials=""))
        self._server.start(path=self.script_path(
            "v4x4", "tx_pull_1_nested_list.script"))
        session = driver.session("w", fetch_size=1)
        tx = session.begin_transaction()
        res1 = tx.run("CYPHER")
        seq = []
        seqs = []
        while True:
            rec1 = res1.next()
            if isinstance(rec1, types.NullRecord):
                break
            seq.append(rec1.values[0].value)
            seq2 = []
            for rec2 in tx.run("CYPHER NESTED").list():
                seq2.append(rec2.values[0].value)
            seqs.append(seq2)

        tx.commit()
        driver.close()
        self._server.done()
        self.assertEqual(["1_1", "1_2"], seq)
        self.assertEqual([["2_1", "2_2"], ["3_1", "3_2"]], seqs)
