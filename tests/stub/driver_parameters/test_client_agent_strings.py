import abc
from contextlib import contextmanager

import nutkit.protocol as types
from nutkit.frontend import Driver
from tests.shared import TestkitTestCase
from tests.stub.shared import StubServer


class _ClientAgentStringsTestBase(TestkitTestCase, abc.ABC):

    @property
    @abc.abstractmethod
    def version_folder(self) -> str:
        ...

    def setUp(self):
        super().setUp()
        self._server = StubServer(9000)
        self._driver = None
        self._session = None

    def tearDown(self):
        self._server.reset()
        if self._session:
            self._session.close()
        if self._driver:
            self._driver.close()
        return super().tearDown()

    def _start_server(self, script, version_folder=None, vars_=None):
        if version_folder is None:
            version_folder = self.version_folder
        self._server.start(self.script_path(version_folder, script),
                           vars_=vars_)

    @contextmanager
    def driver(self, **kwargs):
        auth = types.AuthorizationToken("basic", principal="neo4j",
                                        credentials="pass")
        uri = "bolt://%s" % self._server.address
        driver = Driver(self._backend, uri, auth, **kwargs)
        try:
            yield driver
        finally:
            driver.close()

    @contextmanager
    def session(self, driver=None, **driver_kwargs):
        if driver is None:
            with self.driver(**driver_kwargs) as driver:
                session = driver.session("r")
                try:
                    yield session
                finally:
                    session.close()
        else:
            session = driver.session("r")
            try:
                yield session
            finally:
                session.close()

    def _session_run_return_1(self, **driver_kwargs):
        with self.session(**driver_kwargs) as session:
            result = session.run("RETURN 1 AS n")
            list(result)

    def _test_default_user_agent(self):
        self._start_server("user_agent_default.script")
        self._session_run_return_1()

    def _test_custom_user_agent(self):
        self._start_server("user_agent_custom.script")
        self._session_run_return_1(user_agent="Hello, I'm a banana 🍌!")

    def _test_empty_user_agent(self):
        self._start_server("user_agent_empty.script")
        self._session_run_return_1(user_agent="")


class TestClientAgentStringsV5x2(_ClientAgentStringsTestBase):

    version_folder = "v5x2"
    required_features = (types.Feature.BOLT_5_2,)

    def test_default_user_agent(self):
        super()._test_default_user_agent()

    def test_custom_user_agent(self):
        super()._test_custom_user_agent()

    def test_empty_user_agent(self):
        super()._test_empty_user_agent()


class TestClientAgentStringsV5x3(_ClientAgentStringsTestBase):

    version_folder = "v5x3"
    required_features = (types.Feature.BOLT_5_3,)

    def test_default_user_agent(self):
        super()._test_default_user_agent()

    def test_custom_user_agent(self):
        super()._test_custom_user_agent()

    def test_empty_user_agent(self):
        super()._test_empty_user_agent()
