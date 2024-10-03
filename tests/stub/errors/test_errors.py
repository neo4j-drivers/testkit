import json
from abc import (
    ABC,
    abstractmethod,
)
from contextlib import contextmanager
from copy import deepcopy

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


DEFAULT_DIAG_REC = {
    "CURRENT_SCHEMA": "/",
    "OPERATION": "",
    "OPERATION_CODE": "0",
}


class TestError5x6(_ErrorTestCase):
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
                error_data = {
                    "code": error_code,
                    "message": error_message,
                }

                exc = self.get_error(error_data)

                self.assertEqual(exc.code, error_code)
                self.assertEqual(exc.msg, error_message)
                self.assertEqual(exc.retryable, retryable)
                if self.driver_supports_features(types.Feature.BOLT_5_7):
                    self.assertEqual(exc.gql_status, "50N42")
                    self.assertEqual(
                        exc.status_description,
                        "error: "
                        "general processing exception - unexpected error. "
                        f"{error_message}",
                    )
                    self.assertEqual(
                        exc.diagnostic_record,
                        types.as_cypher_type(DEFAULT_DIAG_REC).value,
                    )
                    self.assertEqual(exc.raw_classification, None)
                    self.assertEqual(exc.classification, "UNKNOWN")
                    self.assertIsNone(exc.cause)
                    if self.driver_supports_features(
                        types.Feature.API_RETRYABLE_EXCEPTION
                    ):
                        self.assertEqual(exc.retryable, retryable)


class TestError5x7(_ErrorTestCase):
    required_features = (
        types.Feature.BOLT_5_7,
    )

    bolt_version = "5.7"

    def _make_test_error_data(
        self,
        status=...,
        description=...,
        message=...,
        code=...,
        diagnostic_record=...,
        extra_diag_rec=None,
        del_diag_rec=None,
        cause=None,
    ):
        data = {}
        if status is not None:
            data["gql_status"] = "01N00" if status is ... else status
        if description is not None:
            data["description"] = (
                "cool class - mediocre subclass"
                if description is ...
                else description
            )
        if message is not None:
            data["message"] = (
                "Sever ain't cool with this, John Doe!"
                if message is ...
                else message
            )
        if code is not None:
            data["neo4j_code"] = (
                "Neo.ClientError.User.Uncool" if code is ... else code
            )
        if diagnostic_record is ...:
            data["diagnostic_record"] = {
                **DEFAULT_DIAG_REC,
                "_classification": "CLIENT_ERROR",
                "_status_parameters": {"userName": "John Doe"},
            }
        elif diagnostic_record is not None:
            data["diagnostic_record"] = diagnostic_record
        if extra_diag_rec is not None:
            data["diagnostic_record"].update(extra_diag_rec)
        if del_diag_rec:
            for key in del_diag_rec:
                del data["diagnostic_record"][key]
        if cause is not None:
            data["cause"] = cause
        return data

    def _assert_is_test_error(self, exc, data):
        self.assertEqual(exc.gql_status, data["gql_status"])
        self.assertEqual(exc.status_description, data["description"])
        self.assertEqual(exc.msg, data["message"])
        if "neo4j_code" in data:
            self.assertEqual(exc.code, data["neo4j_code"])
        else:
            self.assertFalse(hasattr(exc, "code"))
        expected_diag_rec = deepcopy(data.get("diagnostic_record", {}))
        for k, v in DEFAULT_DIAG_REC.items():
            expected_diag_rec.setdefault(k, v)
        self.assertEqual(
            exc.diagnostic_record,
            types.as_cypher_type(expected_diag_rec).value,
        )
        expected_raw_classification = expected_diag_rec.get("_classification")
        if isinstance(expected_raw_classification, str):
            self.assertEqual(
                exc.raw_classification,
                expected_raw_classification,
            )
        expected_classification = "UNKNOWN"
        if expected_raw_classification in {
            "CLIENT_ERROR",
            "DATABASE_ERROR",
            "TRANSIENT_ERROR",
        }:
            expected_classification = expected_raw_classification
        self.assertEqual(exc.classification, expected_classification)
        if "cause" in data:
            self._assert_is_test_error(exc.cause, data["cause"])

    def test_simple_gql_error(self):
        error_data = self._make_test_error_data()
        error = self.get_error(error_data)
        self._assert_is_test_error(error, error_data)

    def test_nested_gql_error(self):
        for depth in (1, 10):
            with self.subTest(depth=depth):
                cause = None
                for i in range(depth, 0, -1):
                    cause = self._make_test_error_data(
                        status=f"01N{i:02d}",
                        description=f"description ({i})",
                        message=f"message ({i})",
                        code=None,
                        diagnostic_record={
                            "CURRENT_SCHEMA": f"/{i}",
                            "OPERATION": f"OP{i}",
                            "OPERATION_CODE": f"{i}",
                            "_classification": f"CLIENT_ERROR{i}",
                            "_status_parameters": {"nestedCause": i},
                        },
                        cause=cause,
                    )
                error_data = self._make_test_error_data(cause=cause)
                error = self.get_error(error_data)
                self._assert_is_test_error(error, error_data)

    def test_error_classification(self):
        for as_cause in (False, True):
            for classification in (
                "CLIENT_ERROR",
                "DATABASE_ERROR",
                "TRANSIENT_ERROR",
                "SECURITY_ERROR",  # made up classification
            ):
                with self.subTest(
                    as_cause=as_cause,
                    classification=classification,
                ):
                    error_data = self._make_test_error_data(
                        extra_diag_rec={"_classification": classification},
                        code=None if as_cause else ...,
                    )
                    if as_cause:
                        error_data = self._make_test_error_data(
                            cause=error_data,
                        )
                    error = self.get_error(error_data)
                    self._assert_is_test_error(error, error_data)

    def test_filling_default_diagnostic_record(self):
        for as_cause in (False, True):
            with self.subTest(as_cause=as_cause):
                error_data = self._make_test_error_data(
                    diagnostic_record=None,
                    code=None if as_cause else ...,
                )
                if as_cause:
                    error_data = self._make_test_error_data(cause=error_data)
                error = self.get_error(error_data)
                self._assert_is_test_error(error, error_data)

    def test_filling_default_value_in_diagnostic_record(self):
        for as_cause in (False, True):
            for missing_key in (
                "CURRENT_SCHEMA",
                "OPERATION",
                "OPERATION_CODE",
            ):
                with self.subTest(as_cause=as_cause, missing_key=missing_key):
                    error_data = self._make_test_error_data(
                        del_diag_rec=[missing_key],
                        code=None if as_cause else ...,
                    )
                    if as_cause:
                        error_data = self._make_test_error_data(
                            cause=error_data
                        )
                    error = self.get_error(error_data)
                    self._assert_is_test_error(error, error_data)

    def test_keeps_rubbish_in_diagnostic_record(self):
        use_spacial = self.driver_supports_features(
            types.Feature.API_TYPE_SPATIAL
        )
        for as_cause in (False, True):
            with self.subTest(as_cause=as_cause):
                diagnostic_record = {
                    "foo": "bar",
                    "_baz": 1.2,
                    "OPERATION": None,
                    "CURRENT_SCHEMA": {"uh": "oh!"},
                    "OPERATION_CODE": False,
                    "_classification": 42,
                    "_status_parameters": [
                        # stub script will interpret this as JOLT spatial point
                        {"@": "SRID=4326;POINT(56.21 13.43)"}
                        if use_spacial
                        else "whatever",
                    ],
                }
                error_data = self._make_test_error_data(
                    diagnostic_record=diagnostic_record,
                    code=None if as_cause else ...,
                )
                if as_cause:
                    error_data = self._make_test_error_data(cause=error_data)

                error = self.get_error(error_data)

                if use_spacial:
                    diagnostic_record["_status_parameters"] = [
                        types.CypherPoint("wgs84", 56.21, 13.43)
                    ]
                self._assert_is_test_error(error, error_data)

    def test_error_retryable(self):
        for (neo4j_code, retryable) in (
            ("Neo.ClientError.User.Uncool", False),
            ("Neo.TransientError.Oopsie.OhSnap", True),
        ):
            with self.subTest(error_code=neo4j_code):
                error_data = self._make_test_error_data(code=neo4j_code)

                exc = self.get_error(error_data)

                self._assert_is_test_error(exc, error_data)
                self.assertEqual(exc.retryable, retryable)
