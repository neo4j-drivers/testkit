import json

from nutkit import protocol as types
from nutkit.frontend import Driver
from tests.shared import TestkitTestCase
from tests.stub.shared import StubServer


class TestSessionPlan(TestkitTestCase):

    required_features = (types.Feature.BOLT_5_0,
                         types.Feature.API_SESSION_PLAN)

    def setUp(self) -> None:
        super().setUp()
        self._routing_server1 = StubServer(9000)
        self._read_server1 = StubServer(9010)
        self._write_server1 = StubServer(9020)

        self._uri = "neo4j://%s:%d" % (self._routing_server1.host,
                                       self._routing_server1.port)
        self._auth = types.AuthorizationToken(
            "basic", principal="", credentials="")

        self._driver = Driver(self._backend, self._uri, self._auth)
        self._session = None

    def tearDown(self) -> None:
        if self._session is not None:
            self._session.close()
        if self._driver is not None:
            self._driver.close()

        self._write_server1.reset()
        self._read_server1.reset()
        self._routing_server1.reset()

        return super().tearDown()

    def _get_routing_vars(self, host=None):
        if host is None:
            host = self._routing_server1.host
        return {
            "#HOST#": host
        }

    def _start_routing_server1(self, script="router.script", vars_=None):
        if vars_ is None:
            vars_ = self._get_routing_vars()
        self._routing_server1.start(
            path=self.script_path(script),
            vars_=vars_
        )

    def _start_read_server1_with_reader_script(self, query,
                                               autocommit, update):
        self._read_server1.start(
            path=self.script_path("reader.script"),
            vars_={
                "#AUTOCOMMIT#": json.dumps(autocommit),
                "#UPDATE#": json.dumps(update),
                "#QUERY#": query}
        )

    def _start_read_server1_with_reader_two_plan_script(self, query1,
                                                        autocommit1, update1,
                                                        query2, autocommit2,
                                                        update2):
        self._read_server1.start(
            path=self.script_path("reader_two_plan.script"),
            vars_={
                "#AUTOCOMMIT1#": json.dumps(autocommit1),
                "#UPDATE1#": json.dumps(update1),
                "#QUERY1#": query1,
                "#AUTOCOMMIT2#": json.dumps(autocommit2),
                "#UPDATE2#": json.dumps(update2),
                "#QUERY2#": query2}
        )

    def test_should_echo_plan_info(self):
        def _test():
            self._start_routing_server1()
            self._start_read_server1_with_reader_script(
                query, autocommit, update)

            self._session = self._driver.session("w")
            query_characteristics = self._session.plan(query)

            self.assertEqual(query_characteristics.autocommit,
                             autocommit_char)
            self.assertEqual(query_characteristics.update,
                             update_char)

            self._session.close()
            self._session = None

            self._read_server1.done()
            self._routing_server1.done()

        for autocommit in (True, False):
            for update in (True, False):
                query = f"query autocommit={autocommit}, update={update}"
                autocommit_char = _bool_to_autocommit_string(autocommit)
                update_char = _bool_to_update_string(update)
                with self.subTest(
                        autocommit=autocommit, update=update, query=query):
                    _test()
                self._read_server1.reset()
                self._routing_server1.reset()

    def test_should_cache_requests_from_the_same_session(self):
        def _test():
            self._start_routing_server1()
            self._start_read_server1_with_reader_script(
                query, autocommit, update)

            self._session = self._driver.session("w")
            query_characteristics = self._session.plan(query)

            self.assertEqual(query_characteristics.autocommit,
                             autocommit_char)
            self.assertEqual(query_characteristics.update,
                             update_char)

            query_characteristics = self._session.plan(query)

            self.assertEqual(query_characteristics.autocommit,
                             autocommit_char)
            self.assertEqual(query_characteristics.update,
                             update_char)

            self._session.close()
            self._session = None

            self._read_server1.done()
            self._routing_server1.done()

        for autocommit in (True, False):
            for update in (True, False):
                query = f"query autocommit={autocommit}, update={update}"
                autocommit_char = _bool_to_autocommit_string(autocommit)
                update_char = _bool_to_update_string(update)
                with self.subTest(
                        autocommit=autocommit, update=update, query=query):
                    _test()
                self._read_server1.reset()
                self._routing_server1.reset()

    def test_should_cache_requests_from_different_sessions(self):
        def _test():
            self._start_routing_server1()
            self._start_read_server1_with_reader_script(
                query, autocommit, update)

            self._session = self._driver.session("w")
            query_characteristics = self._session.plan(query)

            self.assertEqual(query_characteristics.autocommit,
                             autocommit_char)
            self.assertEqual(query_characteristics.update,
                             update_char)

            self._session.close()
            self._session = None

            self._session = self._driver.session("w")
            query_characteristics = self._session.plan(query)

            self.assertEqual(query_characteristics.autocommit,
                             autocommit_char)
            self.assertEqual(query_characteristics.update,
                             update_char)

            self._session.close()
            self._session = None

            self._read_server1.done()
            self._routing_server1.done()

        for autocommit in (True, False):
            for update in (True, False):
                query = f"query autocommit={autocommit}, update={update}"
                autocommit_char = _bool_to_autocommit_string(autocommit)
                update_char = _bool_to_update_string(update)
                with self.subTest(
                        autocommit=autocommit,
                        update=update,
                        query=query):
                    _test()
                self._read_server1.reset()
                self._routing_server1.reset()

    def test_should_not_mix_the_cached_results(self):
        fist_query = "FIRST QUERY"
        second_query = "SECOND_QUERY"
        self._start_routing_server1()
        self._start_read_server1_with_reader_two_plan_script(
            query1=fist_query, autocommit1=True, update1=False,
            query2=second_query, autocommit2=False, update2=True
        )

        self._session = self._driver.session("w")

        first_query_characteristics = self._session.plan(fist_query)
        self.assertEqual(first_query_characteristics.autocommit, "REQUIRED")
        self.assertEqual(first_query_characteristics.update, "DOES_NOT_UPDATE")

        second_query_characteristics = self._session.plan(second_query)
        self.assertEqual(second_query_characteristics.autocommit, "UNREQUIRED")
        self.assertEqual(second_query_characteristics.update, "UPDATE")

        # THE CACHED SHOULD BE HITTED THIS TIME
        first_query_characteristics = self._session.plan(fist_query)
        self.assertEqual(first_query_characteristics.autocommit, "REQUIRED")
        self.assertEqual(first_query_characteristics.update, "DOES_NOT_UPDATE")

        second_query_characteristics = self._session.plan(second_query)
        self.assertEqual(second_query_characteristics.autocommit, "UNREQUIRED")
        self.assertEqual(second_query_characteristics.update, "UPDATE")

        self._session.close()
        self._session = None
        self._read_server1.done()
        self._routing_server1.done()


def _bool_to_autocommit_string(autocommit):
    return "REQUIRED" if autocommit else "UNREQUIRED"


def _bool_to_update_string(update):
    return "UPDATE" if update else "DOES_NOT_UPDATE"
