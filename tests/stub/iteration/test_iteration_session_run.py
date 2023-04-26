import nutkit.protocol as types
from nutkit.frontend import Driver
from tests.shared import (
    driver_feature,
    get_driver_name,
    TestkitTestCase,
)
from tests.stub.shared import StubServer


class TestIterationSessionRun(TestkitTestCase):

    def setUp(self):
        super().setUp()
        self._server = StubServer(9001)

    def tearDown(self):
        self._server.reset()
        super().tearDown()

    def _run(self, n, script_fn, expected_sequence, expected_error=False,
             protocol_version="v4x4"):
        uri = "bolt://%s" % self._server.address
        driver = Driver(self._backend, uri,
                        types.AuthorizationToken("basic", principal="",
                                                 credentials=""))
        self._server.start(path=self.script_path(protocol_version, script_fn))
        session = driver.session("w", fetch_size=n)
        result = session.run("RETURN 1 AS n")
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
        driver.close()
        self._server.done()
        self.assertEqual(expected_sequence, sequence)
        self.assertEqual(expected_error, got_error)

    # Last fetched batch is a full batch
    @driver_feature(types.Feature.BOLT_4_4)
    def test_full_batch(self):
        self._run(2, "pull_2_end_full_batch.script",
                  ["1", "2", "3", "4", "5", "6"])

    # Last fetched batch is half full (or more important not full)
    @driver_feature(types.Feature.BOLT_4_4)
    def test_half_batch(self):
        self._run(2, "pull_2_end_half_batch.script", ["1", "2", "3", "4", "5"])

    # Last fetched batch is empty
    @driver_feature(types.Feature.BOLT_4_4)
    def test_empty_batch(self):
        self._run(2, "pull_2_end_empty_batch.script", ["1", "2", "3", "4"])

    # Last batch returns an error
    @driver_feature(types.Feature.BOLT_4_4)
    def test_error(self):
        self._run(2, "pull_2_end_error.script", ["1", "2", "3", "4", "5"],
                  expected_error=True)

    # Support -1, not batched at all
    @driver_feature(types.Feature.BOLT_4_4)
    def test_all(self):
        self._run(-1, "pull_all.script", ["1", "2", "3", "4", "5", "6"])

    def test_all_slow_connection(self):
        self._run(-1, "pull_all_slow_connection.script",
                  ["1", "2", "3", "4", "5", "6"])

    # Support -1, not batched at all for BOLTv3
    @driver_feature(types.Feature.BOLT_3_0)
    def test_all_v3(self):
        self._run(-1, "pull_all.script", ["1", "2", "3", "4", "5", "6"],
                  protocol_version="v3")

    def test_discards_on_session_close(self):
        def test(version_, script_):
            uri = "bolt://%s" % self._server.address
            driver = Driver(self._backend, uri,
                            types.AuthorizationToken("basic", principal="",
                                                     credentials=""))
            self._server.start(
                path=self.script_path(version_, script_),
                vars_={"#MODE#": mode[0]}
            )
            try:
                session = driver.session(mode[0], fetch_size=2)
                session.run("RETURN 1 AS n").next()
                self.assertEqual(self._server.count_requests("DISCARD"), 0)
                session.close()
                self._server.done()
                if (version_ == "v4x4"
                        and get_driver_name() not in ["java", "javascript",
                                                      "ruby"]):
                    # assert only JAVA and JS pulls results eagerly.
                    self.assertEqual(self._server.count_requests("PULL"), 1)
                driver.close()
            finally:
                self._server.reset()

        for version, script in (("v3", "pull_all_any_mode.script"),
                                ("v4x4", "pull_2_then_discard.script")):
            if not self.driver_supports_bolt(version):
                continue
            # TODO: remove this block once all drivers work
            if version == "v4x4" and get_driver_name() in ["javascript"]:
                # driver would eagerly pull all available results in the
                # background
                continue
            for mode in ("write", "read"):
                with self.subTest(version=version, mode=mode):
                    test(version, script)

    @driver_feature(types.Feature.BOLT_4_4)
    def test_nested(self):
        uri = "bolt://%s" % self._server.address
        driver = Driver(self._backend, uri,
                        types.AuthorizationToken("basic", principal="",
                                                 credentials=""))
        self._server.start(path=self.script_path("v4x4",
                                                 "pull_1_nested.script"))
        session = driver.session("w", fetch_size=1)
        res1 = session.run("CYPHER")
        seq = []
        seqs = []
        i = 0
        while True:
            rec1 = res1.next()
            if isinstance(rec1, types.NullRecord):
                break
            seq.append(rec1.values[0].value)
            res2 = session.run("CYPHER NESTED %d" % i)
            seq2 = [rec.values[0].value for rec in res2]
            seqs.append(seq2)
            i += 1

        driver.close()
        self._server.done()
        self.assertEqual(["1_1", "1_2", "1_3"], seq)
        self.assertEqual([["2_1", "2_2"], ["3_1"], ["4_1"]], seqs)

    @driver_feature(types.Feature.BOLT_4_4, types.Feature.API_RESULT_LIST,
                    types.Feature.OPT_RESULT_LIST_FETCH_ALL)
    def test_nested_using_list(self):
        uri = "bolt://%s" % self._server.address
        driver = Driver(self._backend, uri,
                        types.AuthorizationToken("basic", principal="",
                                                 credentials=""))
        self._server.start(
            path=self.script_path("v4x4", "pull_1_nested_list.script")
        )
        session = driver.session("w", fetch_size=1)
        res1 = session.run("CYPHER")
        seq = []
        seqs = []
        i = 0
        while True:
            rec1 = res1.next()
            if isinstance(rec1, types.NullRecord):
                break
            seq.append(rec1.values[0].value)
            seq2 = [rec.values[0].value
                    for rec in session.run("CYPHER NESTED %d" % i).list()]
            seqs.append(seq2)
            i += 1

        driver.close()
        self._server.done()
        self.assertEqual(["1_1", "1_2", "1_3"], seq)
        self.assertEqual([["2_1", "2_2"], ["3_1", "3_2"], ["4_1", "4_2"]],
                         seqs)
