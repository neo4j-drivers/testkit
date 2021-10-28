from nutkit.frontend import Driver
import nutkit.protocol as types
from tests.shared import (
    TestkitTestCase,
    get_driver_name,
)
from tests.stub.shared import StubServer


class TestIterationSessionRun(TestkitTestCase):

    required_features = types.Feature.BOLT_4_0,

    def setUp(self):
        super().setUp()
        self._server = StubServer(9001)

    def tearDown(self):
        self._server.reset()
        super().tearDown()

    def _run(self, n, script_fn, expected_sequence, expected_error=False,
             protocol_version="v4x0"):
        uri = "bolt://%s" % self._server.address
        driver = Driver(self._backend, uri,
                        types.AuthorizationToken("basic", principal="",
                                                 credentials=""))
        self._server.start(path=self.script_path(protocol_version, script_fn))
        session = driver.session("w", fetchSize=n)
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
    def test_full_batch(self):
        self._run(2, "pull_2_end_full_batch.script", ["1", "2", "3", "4", "5", "6"])

    # Last fetched batch is half full (or more important not full)
    def test_half_batch(self):
        self._run(2, "pull_2_end_half_batch.script", ["1", "2", "3", "4", "5"])

    # Last fetched batch is empty
    def test_empty_batch(self):
        self._run(2, "pull_2_end_empty_batch.script", ["1", "2", "3", "4"])

    # Last batch returns an error
    def test_error(self):
        self._run(2, "pull_2_end_error.script", ["1", "2", "3", "4", "5"],
                  expected_error=True)

    # Support -1, not batched at all
    def test_all(self):
        self._run(-1, "pull_all.script", ["1", "2", "3", "4", "5", "6"])

    def test_all_slow_connection(self):
        self._run(-1, "pull_all_slow_connection.script",
                  ["1", "2", "3", "4", "5", "6"])

    # Support -1, not batched at all for BOLTv3
    def test_all_v3(self):
        self._run(-1, "pull_all.script", ["1", "2", "3", "4", "5", "6"],
                  protocol_version="v3")

    def test_discards_on_session_close(self):
        def test():
            uri = "bolt://%s" % self._server.address
            driver = Driver(self._backend, uri,
                            types.AuthorizationToken("basic", principal="",
                                                     credentials=""))
            self._server.start(
                path=self.script_path(version, script),
                vars={"#MODE#": mode[0]}
            )
            try:
                session = driver.session(mode[0], fetchSize=2)
                session.run("RETURN 1 AS n").next()
                self.assertEqual(self._server.count_requests("DISCARD"), 0)
                session.close()
                self._server.done()
                if (version == "v4x0"
                        and get_driver_name() not in ["java", "javascript"]):
                    # assert only JAVA and JS pulls results eagerly.
                    self.assertEqual(self._server.count_requests("PULL"), 1)
                driver.close()
            finally:
                self._server.reset()

        for version, script in (("v3", "pull_all_any_mode.script"),
                                ("v4x0", "pull_2_then_discard.script")):
            # TODO: remove this block once all drivers work
            if version == "v4x0" and get_driver_name() in ["javascript"]:
                # driver would eagerly pull all available results in the
                # background
                continue
            for mode in ("write", "read"):
                with self.subTest(version + "-" + mode):
                    test()
