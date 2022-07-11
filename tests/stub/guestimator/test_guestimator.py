import json

from nutkit import protocol as types
from nutkit.frontend import Driver
from tests.shared import TestkitTestCase
from tests.stub.shared import StubServer


class TestDriverPlan(TestkitTestCase):

    required_features = (types.Feature.BOLT_5_0,
                         types.Feature.BACKEND_FETCH_PLAN)

    def setUp(self) -> None:
        super().setUp()
        self._routing_server1 = StubServer(9000)
        self._read_server1 = StubServer(9010)
        self._write_server1 = StubServer(9020)

        self._uri = "neo4j://%s:%d" % (self._routing_server1.host,
                                       self._routing_server1.port)
        self._auth = types.AuthorizationToken(
            "basic", principal="", credentials="")

        self._driver = self._create_driver()

    def tearDown(self) -> None:
        if self._driver is not None:
            self._driver.close()
            self._driver = None

        self._write_server1.reset()
        self._read_server1.reset()
        self._routing_server1.reset()

        return super().tearDown()

    def _create_driver(self, **config):
        return Driver(self._backend, self._uri, self._auth, **config)

    def _get_routing_vars(self, host=None, database=None):
        if host is None:
            host = self._routing_server1.host
        if database is None:
            database = "adb"
        return {
            "#HOST#": host,
            "#DATABASE#": database
        }

    def _start_routing_server1(self, script="router.script", database=None,
                               vars_=None):
        if vars_ is None:
            vars_ = self._get_routing_vars(database=database)
        self._routing_server1.start(
            path=self.script_path(script),
            vars_=vars_
        )

    def _start_read_server1_with_reader_script(self, query,
                                               autocommit, update, database,
                                               params):
        params_field = f', "params":{params}'\
            if params is not None else ""
        self._read_server1.start(
            path=self.script_path("reader.script"),
            vars_={
                "#AUTOCOMMIT#": json.dumps(autocommit),
                "#UPDATE#": json.dumps(update),
                "#QUERY#": query,
                "#DATABASE#": database,
                "#PARAMS#": params_field
            })

    def test_should_echo_plan_info(self):
        def _test():
            self.setUp()
            try:
                database_resolved_name = database or "adb"
                params_script = json.dumps(server_params) \
                    if params is not None else None
                self._start_routing_server1(database=database_resolved_name)
                self._start_read_server1_with_reader_script(
                    query, autocommit, update, database_resolved_name,
                    params_script)

                plan = self._driver.plan(query, database, params)

                self.assertEqual(plan.autocommit, autocommit)
                self.assertEqual(plan.update, update)

                self._read_server1.done()
                self._routing_server1.done()
            finally:
                self.tearDown()

        self.tearDown()

        string_params = ({"xs": types.CypherString("")}, {"xs": ""})
        for autocommit in (True, False):
            for update in (True, False):
                for database in (None, "somedb"):
                    for (params, server_params) in (
                            (None, None), string_params):
                        query = "RETURN fancycall($xs)"
                        with self.subTest(
                                autocommit=autocommit,
                                update=update,
                                query=query,
                                database=database,
                                params=params,
                                server_params=server_params):
                            _test()
                        self._read_server1.reset()
                        self._routing_server1.reset()

    def test_should_supports_all_datatypes_as_params(self):
        def _test():
            self.setUp()
            try:
                self._start_routing_server1()
                self._start_read_server1_with_reader_script(
                    query="RETURN $x",
                    autocommit=True,
                    update=True,
                    database="adb",
                    params=jolt_params)

                plan = self._driver.plan("RETURN $x", params=params)

                self.assertEqual(plan.autocommit, True)
                self.assertEqual(plan.update, True)

                self._read_server1.done()
                self._routing_server1.done()
            finally:
                self.tearDown()

        self.tearDown()
        for (param, jolt_param) in self._all_supported_datatypes_fixture():
            params = {"x": param}
            jolt_params = '{"x": %s}' % jolt_param
            with self.subTest(params=params):
                _test()

    def _all_supported_datatypes_fixture(self):
        non_reductable_datatypes = [
            (types.CypherBool(True), "true"),
            (types.CypherNull(), "null")
        ]
        return non_reductable_datatypes
