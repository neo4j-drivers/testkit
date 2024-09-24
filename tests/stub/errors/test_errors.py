import json
from abc import (
    ABC,
    abstractmethod,
)
from contextlib import contextmanager

import nutkit.protocol as types
from nutkit.frontend import Driver
from tests.shared import TestkitTestCase
from tests.stub.shared import StubServer


class _ErrorTestCase(TestkitTestCase, ABC):
    @property
    @abstractmethod
    def bolt_version(self) -> str:
        pass

    @contextmanager
    def server(self, script, vars_=None):
        if vars_ is None:
            vars_ = {}
        vars_.update({"#BOLT_VERSION#": self.bolt_version})
        server = StubServer(9001)
        server.start(path=self.script_path(script),
                     vars_=vars_)
        try:
            yield server
        except Exception:
            server.reset()
            raise

        server.done()

    @contextmanager
    def driver(self, server):
        auth = types.AuthorizationToken("bearer", credentials="foo")
        uri = f"bolt://{server.address}"
        driver = Driver(self._backend, uri, auth)
        try:
            yield driver
        finally:
            driver.close()

    @contextmanager
    def session(self, driver):
        session = driver.session("w")
        try:
            yield session
        finally:
            session.close()

    def get_error(self, error_data):
        def run(session_):
            session_.run("RETURN 1").consume()

        vars_ = {"#ERROR#": json.dumps(error_data)}
        with self.server("error.script", vars_=vars_) as server:
            with self.driver(server) as driver:
                with self.session(driver) as session:
                    with self.assertRaises(types.DriverError) as exc:
                        run(session)
                    return exc.exception


class TestError5x5(_ErrorTestCase):
    required_features = (
        types.Feature.BOLT_5_6,
    )

    bolt_version = "5.6"

    def test_error(self):
        for (error_code, retryable) in (
            ("Neo.ClientError.User.Uncool", False),
            ("Neo.TransientError.Oopsie.OhSnap", True),
        ):
            with self.subTest(code=error_code):
                error_message = "Sever ain't cool with this!"
                error_data = {"code": error_code, "message": error_message}

                error = self.get_error(error_data)

                self.assertEqual(error.code, error_code)
                self.assertEqual(error.msg, error_message)
                self.assertEqual(error.retryable, retryable)
                if self.driver_supports_features(types.Feature.BOLT_5_6):
                    self.assertEqual(error.gql_status, "50N42")
                    expected_desc = (
                        "error: "
                        "general processing exception - unknown error. "
                        f"{error_message}"
                    )
                    self.assertEqual(error.status_description, expected_desc)
                    self.assertIsNone(error.cause)
                    # TODO: TBD
                    # self.assertEqual(error.classification, "UNKNOWN")


DEFAULT_DIAG_REC = {
    "CURRENT_SCHEMA": "/",
    "OPERATION": "",
    "OPERATION_CODE": "0",
}


class TestError5x7(_ErrorTestCase):
    required_features = (
        types.Feature.BOLT_5_7,
    )

    bolt_version = "5.7"

    def test_error(self):
        error_status = "01N00"
        error_message = "Sever ain't cool with this, John Doe!"
        error_description = "cool class - mediocre subclass"
        error_code = "Neo.ClientError.User.Uncool"
        diagnostic_record = {
            "CURRENT_SCHEMA": "/",
            "OPERATION": "",
            "OPERATION_CODE": "0",
            "_classification": "CLIENT_ERROR",
            "_status_parameters": {
                "userName": "John Doe",
            },
        }
        error_data = {
            "gql_status": error_status,
            "message": error_message,
            "description": error_description,
            "code": error_code,
            "diagnostic_record": diagnostic_record,
        }

        error = self.get_error(error_data)
        self.assertEqual(error.code, error_code)
        self.assertEqual(error.msg, error_message)
        # TODO: what part of the error is used to determine retryability?
        # self.assertEqual(error.retryable, retryable)
        self.assertEqual(error.gql_status, error_status)
        self.assertEqual(
            error.status_description,
            error_description
        )
        self.assertIsNone(error.cause)
        self.assertEqual(error.diagnostic_record,
                         diagnostic_record)
        self.assertEqual(error.classification, "CLIENT_ERROR")

    def test_nested_error(self):
        error_status = "01ABC"
        error_code = "Neo.ClientError.Bar.Baz"
        cause_status = "01N00"
        cause_message = "Sever ain't cool with this, John Doe!"
        cause_description = "cool class - mediocre subclass"
        cause_code = "Neo.ClientError.User.Uncool"
        diagnostic_record = {
            "CURRENT_SCHEMA": "/",
            "OPERATION": "",
            "OPERATION_CODE": "0",
            "_classification": "CLIENT_ERROR",
            "_status_parameters": {
                "userName": "John Doe",
            },
        }
        error_data = {
            "gql_status": error_status,
            "message": "msg",
            "description": "description",
            "code": error_code,
            "diagnostic_record": DEFAULT_DIAG_REC,
            "cause": {
                "gql_status": cause_status,
                "message": cause_message,
                "description": cause_description,
                "code": cause_code,
                "diagnostic_record": diagnostic_record,
            },
        }

        error = self.get_error(error_data)

        self.assertIsInstance(error, types.DriverError)
        self.assertEqual(error.code, error_code)

        cause = error.cause
        self.assertIsNotNone(cause)
        self.assertEqual(cause.code, cause_code)
        self.assertEqual(cause.msg, cause_message)
        # TODO: self.assertEqual(cause.retryable, ?)
        self.assertEqual(cause.gql_status, cause_status)
        self.assertEqual(cause.status_description,
                         cause_description)
        self.assertIsNone(cause.cause)
        self.assertEqual(cause.diagnostic_record,
                         diagnostic_record)
        # TODO: TBD
        # self.assertEqual(cause.classification, "UNKNOWN")

    def test_deeply_nested_error(self):
        def make_status(i_):
            return f"01N{i_:02d}"

        error_data = {
            "gql_status": make_status(0),
            "message": "msg",
            "description": "explanation",
            "code": "Neo.ClientError.Bar.Baz0",
            "diagnostic_record": DEFAULT_DIAG_REC,
        }
        parent_data = error_data
        for i in range(1, 10):
            parent_data["cause"] = {
                "gql_status": make_status(i),
                "message": f"msg{i}",
                "description": f"explanation{i}",
                "code": f"Neo.ClientError.Bar.Baz{i}",
                "diagnostic_record": DEFAULT_DIAG_REC,
            }
            parent_data = parent_data["cause"]

        error = self.get_error(error_data)

        for i in range(10):
            self.assertIsInstance(error, types.DriverError)
            self.assertEqual(error.code, f"Neo.ClientError.Bar.Baz{i}")
            self.assertEqual(error.gql_status, make_status(i))
            error = error.cause
