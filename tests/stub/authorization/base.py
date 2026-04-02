import inspect
import json
import os
from dataclasses import dataclass

import nutkit.protocol as types
from nutkit.frontend import AuthTokenManager
from tests.shared import (
    get_driver_name,
    TestkitTestCase,
)


class AuthorizationBase(TestkitTestCase):
    # While there is no unified language agnostic error type mapping, a
    # dedicated driver mapping is required to determine if the expected
    # error is returned.
    def assert_is_authorization_error(self, error, retryable=False):
        self.assertEqual(self._RAW_AUTH_EXPIRED["code"], error.code)

        if retryable:
            return self._assert_is_retryable_authorization_error(error)

        driver = get_driver_name()
        expected_type = None
        if driver in ["java"]:
            expected_type = \
                "org.neo4j.driver.exceptions.AuthorizationExpiredException"
        elif driver in ["python"]:
            expected_type = "<class 'neo4j.exceptions.TransientError'>"
        elif driver in ["javascript"]:
            pass
        elif driver in ["dotnet"]:
            expected_type = "AuthorizationExpired"
        elif driver in ["go"]:
            expected_type = "Neo4jError"
        elif driver in ["ruby"]:
            expected_type = \
                "Neo4j::Driver::Exceptions::AuthorizationExpiredException"
        else:
            self.fail("no error mapping is defined for %s driver" % driver)
        if expected_type is not None:
            self.assertEqual(expected_type, error.errorType)
        # This exception should always be considered retryable
        self._assert_retryable(error)

    def _assert_is_retryable_authorization_error(self, error):
        driver = get_driver_name()
        expected_type = None
        if driver in ["python"]:
            expected_type = "<class 'neo4j.exceptions.TransientError'>"
        elif driver in ["javascript"]:
            pass
        elif driver in ["dotnet"]:
            expected_type = "AuthorizationExpired"
        elif driver in ["java"]:
            expected_type = \
                "org.neo4j.driver.exceptions.SecurityRetryableException"
        elif driver in ["go"]:
            expected_type = "Neo4jError"
        else:
            self.fail("no error mapping is defined for %s driver" % driver)
        if expected_type is not None:
            self.assertEqual(expected_type, error.errorType)
        self._assert_retryable(error)

    def assert_is_token_error(self, error, retryable=False):
        self.assertEqual(self._RAW_TOKEN_EXPIRED["code"], error.code)
        self.assertIn(self._RAW_TOKEN_EXPIRED["message"], error.msg)

        if retryable:
            return self._assert_is_retryable_token_error(error)

        driver = get_driver_name()
        expected_type = None
        if driver in ["python"]:
            expected_type = "<class 'neo4j.exceptions.TokenExpired'>"
        elif driver in ["go"]:
            expected_type = "TokenExpiredError"
        elif driver in ["javascript"]:
            pass
        elif driver == "java":
            expected_type = "org.neo4j.driver.exceptions.TokenExpiredException"
        elif driver == "ruby":
            expected_type = "Neo4j::Driver::Exceptions::TokenExpiredException"
        elif driver == "dotnet":
            expected_type = "ClientError"
        elif driver == "go":
            expected_type = "TokenExpiredError"
        else:
            self.fail("no error mapping is defined for %s driver" % driver)
        if expected_type is not None:
            self.assertEqual(expected_type, error.errorType)
        self._assert_not_retryable(error)

    def _assert_is_retryable_token_error(self, error):
        driver = get_driver_name()
        expected_type = None
        if driver in ["python"]:
            expected_type = "<class 'neo4j.exceptions.TokenExpired'>"
        elif driver in ["javascript"]:
            pass
        elif driver in ["dotnet"]:
            expected_type = "ClientError"
        elif driver in ["java"]:
            expected_type = \
                "org.neo4j.driver.exceptions.SecurityRetryableException"
        elif driver == "go":
            expected_type = "TokenExpiredError"
        else:
            self.fail("no error mapping is defined for %s driver" % driver)
        if expected_type is not None:
            self.assertEqual(expected_type, error.errorType)
        self._assert_retryable(error)

    def assert_is_unauthorized_error(self, error, retryable=False):
        self.assertEqual(self._RAW_UNAUTHORIZED["code"], error.code)
        self.assertIn(self._RAW_UNAUTHORIZED["message"], error.msg)

        if retryable:
            return self._assert_is_retryable_unauthorized_error(error)

        driver = get_driver_name()
        expected_type = None
        if driver in ["python"]:
            expected_type = "<class 'neo4j.exceptions.AuthError'>"
        elif driver in ["javascript"]:
            pass
        elif driver in ["dotnet"]:
            expected_type = "AuthenticationError"
        elif driver in ["java"]:
            expected_type = \
                "org.neo4j.driver.exceptions.AuthenticationException"
        elif driver in ["go"]:
            expected_type = "Neo4jError"
        else:
            self.fail("no error mapping is defined for %s driver" % driver)
        if expected_type is not None:
            self.assertEqual(expected_type, error.errorType)
        self._assert_not_retryable(error)

    def _assert_is_retryable_unauthorized_error(self, error):
        driver = get_driver_name()
        expected_type = None
        if driver in ["python"]:
            expected_type = "<class 'neo4j.exceptions.AuthError'>"
        elif driver in ["javascript"]:
            pass
        elif driver in ["dotnet"]:
            expected_type = "AuthenticationError"
        elif driver in ["java"]:
            expected_type = \
                "org.neo4j.driver.exceptions.SecurityRetryableException"
        elif driver in ["go"]:
            expected_type = "Neo4jError"
        else:
            self.fail("no error mapping is defined for %s driver" % driver)
        if expected_type is not None:
            self.assertEqual(expected_type, error.errorType)
        self._assert_retryable(error)

    def assert_is_security_error(self, error, retryable=False):
        self.assertEqual(self._RAW_SECURITY_EXC["code"], error.code)
        self.assertIn(self._RAW_SECURITY_EXC["message"], error.msg)

        if retryable:
            return self._assert_is_retryable_security_error(error)

        driver = get_driver_name()
        expected_type = None
        if driver in ["python"]:
            expected_type = "<class 'neo4j.exceptions.ClientError'>"
        elif driver in ["javascript"]:
            pass
        elif driver in ["dotnet"]:
            expected_type = "OtherSecurityException"
        elif driver in ["java"]:
            expected_type = "org.neo4j.driver.exceptions.SecurityException"
        elif driver in ["go"]:
            expected_type = "Neo4jError"
        else:
            self.fail("no error mapping is defined for %s driver" % driver)
        if expected_type is not None:
            self.assertEqual(expected_type, error.errorType)
        self._assert_not_retryable(error)

    def _assert_is_retryable_security_error(self, error):
        driver = get_driver_name()
        expected_type = None
        if driver in ["python"]:
            expected_type = "<class 'neo4j.exceptions.ClientError'>"
        elif driver in ["javascript"]:
            pass
        elif driver in ["java"]:
            expected_type = \
                "org.neo4j.driver.exceptions.SecurityRetryableException"
        elif driver in ["dotnet"]:
            expected_type = "OtherSecurityException"
        elif driver in ["go"]:
            expected_type = "Neo4jError"
        else:
            self.fail("no error mapping is defined for %s driver" % driver)
        if expected_type is not None:
            self.assertEqual(expected_type, error.errorType)
        self._assert_retryable(error)

    def assert_is_transient_error(self, error):
        self.assertEqual(self._RAW_TRANSIENT_EXC["code"], error.code)
        self.assertIn(self._RAW_TRANSIENT_EXC["message"], error.msg)

        driver = get_driver_name()
        expected_type = None
        if driver in ["python"]:
            expected_type = "<class 'neo4j.exceptions.TransientError'>"
        elif driver in ["javascript"]:
            pass
        elif driver in ["java"]:
            expected_type = "org.neo4j.driver.exceptions.TransientException"
        elif driver in ["dotnet"]:
            expected_type = "DriverError"
        elif driver in ["go"]:
            expected_type = "Neo4jError"
        else:
            self.fail("no error mapping is defined for %s driver" % driver)
        if expected_type is not None:
            self.assertEqual(expected_type, error.errorType)
        self._assert_retryable(error)

    def assert_is_random_error(self, error):
        self.assertEqual(self._RAW_RANDOM_EXC["code"], error.code)
        self.assertIn(self._RAW_RANDOM_EXC["message"], error.msg)

        driver = get_driver_name()
        expected_type = None
        if driver in ["python"]:
            expected_type = "<class 'neo4j.exceptions.ClientError'>"
        elif driver in ["javascript"]:
            pass
        elif driver in ["java"]:
            expected_type = "org.neo4j.driver.exceptions.ClientException"
        elif driver in ["dotnet"]:
            expected_type = "ClientError"
        elif driver in ["go"]:
            expected_type = "Neo4jError"
        else:
            self.fail("no error mapping is defined for %s driver" % driver)
        if expected_type is not None:
            self.assertEqual(expected_type, error.errorType)
        self._assert_not_retryable(error)

    def assert_re_auth_unsupported_error(self, error):
        self.assertIsInstance(error, types.DriverError)
        driver = get_driver_name()
        if driver in ["python"]:
            self.assertEqual(
                "<class 'neo4j.exceptions.ConfigurationError'>",
                error.errorType
            )
            self.assertIn(
                "user switching is not supported for bolt protocol "
                "5.0",
                error.msg.lower()
            )
        elif driver in ["javascript"]:
            self.assertEqual(
                "N/A",
                error.code
            )
            self.assertEqual(
                "Driver is connected to a database that does not support "
                "user switch.",
                error.msg
            )
        elif driver in ["java"]:
            self.assertEqual(
                "org.neo4j.driver.exceptions.UnsupportedFeatureException",
                error.errorType
            )
        elif driver in ["ruby"]:
            self.assertEqual(
                "Neo4j::Driver::Exceptions::UnsupportedFeatureException",
                error.errorType
            )
        elif driver in ["dotnet"]:
            self.assertEqual("UnsupportedFeatureException", error.errorType)
        elif driver in ["go"]:
            self.assertEqual("feature not supported", error.errorType)
            self.assertIn("session auth", error.msg)
        else:
            self.fail("no error mapping is defined for %s driver" % driver)

    def _assert_not_retryable(self, error):
        if self.driver_supports_features(
            types.Feature.API_RETRYABLE_EXCEPTION
        ):
            self.assertFalse(error.retryable)

    def _assert_retryable(self, error):
        if self.driver_supports_features(
            types.Feature.API_RETRYABLE_EXCEPTION
        ):
            self.assertTrue(error.retryable)

    _RAW_AUTH_EXPIRED = {
        "code": "Neo.ClientError.Security.AuthorizationExpired",
        "message": "Authorization expired",
    }
    _RAW_TOKEN_EXPIRED = {
        "code": "Neo.ClientError.Security.TokenExpired",
        "message": "Token expired",
    }
    _RAW_UNAUTHORIZED = {
        "code": "Neo.ClientError.Security.Unauthorized",
        "message": "Wrong credentials. Kthxbye!",
    }
    _RAW_SECURITY_EXC = {
        "code": "Neo.ClientError.Security.MadeUp",
        "message": r"Some security issue ¯\_(ツ)_/¯",
    }
    _RAW_TRANSIENT_EXC = {
        "code": "Neo.TransientError.General.TransactionMemoryLimit",
        "message": "These are not the RAM sticks you're looking for!",
    }
    _RAW_RANDOM_EXC = {
        "code": "Neo.ClientError.Procedure.ProcedureCallFailed",
        "message": "A thing happened...",
    }


class AuthorizationBaseBolt(AuthorizationBase):
    _AUTH_EXPIRED = json.dumps(AuthorizationBase._RAW_AUTH_EXPIRED)
    _TOKEN_EXPIRED = json.dumps(AuthorizationBase._RAW_TOKEN_EXPIRED)
    _UNAUTHORIZED = json.dumps(AuthorizationBase._RAW_UNAUTHORIZED)
    _SECURITY_EXC = json.dumps(AuthorizationBase._RAW_SECURITY_EXC)
    _TRANSIENT_EXC = json.dumps(AuthorizationBase._RAW_TRANSIENT_EXC)
    _RANDOM_EXC = json.dumps(AuthorizationBase._RAW_RANDOM_EXC)

    def _find_version_script(self, script_fns):
        if isinstance(script_fns, str):
            script_fns = [script_fns]
        classes = inspect.getmro(self.__class__)
        tried_locations = []
        for cls in classes:
            cls_vars = None
            if hasattr(cls, "get_vars") and callable(cls.get_vars):
                try:
                    cls_vars = cls.get_vars(self)
                except NotImplementedError:
                    pass
            if not cls_vars or "#VERSION#" not in cls_vars:
                continue
            version_folder = "v{}".format(
                cls_vars["#VERSION#"].replace(".", "x")
            )
            for script_fn in script_fns:
                script_path = self.script_path(version_folder,
                                               script_fn)
                tried_locations.append(script_path)
                if os.path.exists(script_path):
                    return script_path
        raise FileNotFoundError("{!r} tried {!r}".format(
            script_fns, ", ".join(tried_locations)
        ))

    def start_server(self, server, script_fn, vars_=None):
        if vars_ is None:
            vars_ = self.get_vars()
        script_path = self._find_version_script(script_fn)
        server.start(path=script_path, vars_=vars_)

    def script_fn_with_features(self, script_fn):
        has_logon = getattr(self, "has_logon", False)
        minimal = self.driver_supports_features(
            types.Feature.OPT_IMPLICIT_DEFAULT_ARGUMENTS
        )
        auth_pipeline = self.driver_supports_features(
            types.Feature.OPT_AUTH_PIPELINING
        )
        parts = script_fn.rsplit(".", 1)
        if minimal and not has_logon:
            return (
                f"{parts[0]}_pipelined_minimal.{parts[1]}",
                # pipelined is optional, as it makes little sense to have
                # an extra script for it for protocol versions pre
                # LOGOFF/LOGON message (there is nothing to pipeline
                # there).
                f"{parts[0]}_minimal.{parts[1]}",
            )
        elif minimal and auth_pipeline:
            return f"{parts[0]}_pipelined_minimal.{parts[1]}",
        elif auth_pipeline:
            return (
                f"{parts[0]}_pipelined.{parts[1]}",
                f"{parts[0]}.{parts[1]}",
            )
        elif minimal:
            raise RuntimeError(
                "Tests for driver with "
                "types.Feature.OPT_IMPLICIT_DEFAULT_ARGUMENTS but without "
                "types.Feature.OPT_AUTH_PIPELINING are (currently) missing "
                "when logon is supported. "
                "Feel free to add them when needed."
            )
        else:
            return script_fn

    def get_vars(self):
        raise NotImplementedError


@dataclass(frozen=True)
class HandleSecurityExceptionArgs:
    auth: types.AuthorizationToken
    error_code: str


class TrackingAuthTokenManager:
    def __init__(self, backend):
        self._backend = backend
        self._get_auth_count = 0
        self._handle_security_exception_args = []
        self._manager = AuthTokenManager(
            backend, self.get_auth, self.handle_security_exception
        )

    def get_auth(self):
        self._get_auth_count += 1
        return self.raw_get_auth()

    def raw_get_auth(self):
        return types.AuthorizationToken(
            scheme="basic",
            principal="neo4j",
            credentials="pass"
        )

    def handle_security_exception(
        self, auth: types.AuthorizationToken, code: str
    ) -> bool:
        args = HandleSecurityExceptionArgs(auth, code)
        self._handle_security_exception_args.append(args)
        return self._handles_security_exception(code)

    def _handles_security_exception(self, code: str) -> bool:
        return False

    @property
    def get_auth_count(self):
        return self._get_auth_count

    @property
    def handle_security_exception_args(self):
        return self._handle_security_exception_args

    @property
    def handle_security_exception_count(self):
        return len(self._handle_security_exception_args)

    @property
    def manager(self):
        return self._manager
