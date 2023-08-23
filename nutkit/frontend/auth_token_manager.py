from __future__ import annotations

from dataclasses import dataclass
from typing import (
    Any,
    Callable,
    ClassVar,
    Dict,
)

from ..backend import Backend
from ..protocol import (
    AuthorizationToken,
    AuthTokenAndExpiration,
)
from ..protocol import AuthTokenManager as AuthTokenManagerMessage
from ..protocol import (
    AuthTokenManagerClose,
    AuthTokenManagerGetAuthCompleted,
    AuthTokenManagerGetAuthRequest,
    AuthTokenManagerHandleSecurityExceptionCompleted,
    AuthTokenManagerHandleSecurityExceptionRequest,
)
from ..protocol import BasicAuthTokenManager as BasicAuthTokenManagerMessage
from ..protocol import (
    BasicAuthTokenProviderCompleted,
    BasicAuthTokenProviderRequest,
)
from ..protocol import BearerAuthTokenManager as BearerAuthTokenManagerMessage
from ..protocol import (
    BearerAuthTokenProviderCompleted,
    BearerAuthTokenProviderRequest,
    NewAuthTokenManager,
    NewBasicAuthTokenManager,
    NewBearerAuthTokenManager,
)

__all__ = [
    "AuthTokenManager",
    "BasicAuthTokenManager",
    "BearerAuthTokenManager",
]


@dataclass
class AuthTokenManager:
    _registry: ClassVar[Dict[Any, AuthTokenManager]] = {}

    def __init__(
        self,
        backend: Backend,
        get_auth: Callable[[], AuthorizationToken],
        handle_security_exception: Callable[[AuthorizationToken, str], bool]
    ):
        self._backend = backend
        self._get_auth = get_auth
        self._handle_security_exception = handle_security_exception

        req = NewAuthTokenManager()
        res = backend.send_and_receive(req)
        if not isinstance(res, AuthTokenManagerMessage):
            raise Exception(f"Should be AuthTokenManager but was {res}")

        self._auth_token_manager = res
        self._registry[self._auth_token_manager.id] = self

    @property
    def id(self):
        return self._auth_token_manager.id

    @classmethod
    def process_callbacks(cls, request):
        if isinstance(request, AuthTokenManagerGetAuthRequest):
            if request.auth_token_manager_id not in cls._registry:
                raise Exception(
                    "Backend provided unknown Auth Token Manager "
                    f"id: {request.auth_token_manager_id} not found"
                )
            manager = cls._registry[request.auth_token_manager_id]
            auth_token = manager._get_auth()
            return AuthTokenManagerGetAuthCompleted(
                request.id, auth_token
            )
        if isinstance(request, AuthTokenManagerHandleSecurityExceptionRequest):
            if request.auth_token_manager_id not in cls._registry:
                raise Exception(
                    "Backend provided unknown Auth Token Manager "
                    f"id: {request.auth_token_manager_id} not found"
                )
            manager = cls._registry[request.auth_token_manager_id]
            handled = manager._handle_security_exception(request.auth,
                                                         request.error_code)
            return AuthTokenManagerHandleSecurityExceptionCompleted(request.id,
                                                                    handled)

    def close(self, hooks=None):
        res = self._backend.send_and_receive(
            AuthTokenManagerClose(self.id),
            hooks=hooks
        )
        if not isinstance(res, AuthTokenManagerMessage):
            raise Exception(
                f"Should be AuthTokenManager but was {res}"
            )
        del self._registry[self.id]


@dataclass
class BasicAuthTokenManager:
    _registry: ClassVar[Dict[Any, BasicAuthTokenManager]] = {}

    def __init__(
        self,
        backend: Backend,
        callback: Callable[[], AuthorizationToken]
    ):
        self._backend = backend
        self._callback = callback

        req = NewBasicAuthTokenManager()
        res = backend.send_and_receive(req)
        if not isinstance(res, BasicAuthTokenManagerMessage):
            raise Exception(
                f"Should be BasicAuthTokenManager but was {res}"
            )

        self._basic_auth_token_manager = res
        self._registry[self._basic_auth_token_manager.id] = self

    @property
    def id(self):
        return self._basic_auth_token_manager.id

    @classmethod
    def process_callbacks(cls, request):
        if isinstance(request,
                      BasicAuthTokenProviderRequest):
            if (
                request.basic_auth_token_manager_id
                not in cls._registry
            ):
                raise Exception(
                    "Backend provided unknown BasicAuthTokenManager "
                    f"id: {request.basic_auth_token_manager_id} "
                    f"not found"
                )

            manager = cls._registry[
                request.basic_auth_token_manager_id
            ]
            renewable_auth_token = manager._callback()
            return BasicAuthTokenProviderCompleted(
                request.id, renewable_auth_token
            )

    def close(self, hooks=None):
        res = self._backend.send_and_receive(
            AuthTokenManagerClose(self.id),
            hooks=hooks
        )
        if not isinstance(res, AuthTokenManagerMessage):
            raise Exception(f"Should be AuthTokenManager but was {res}")
        del self._registry[self.id]


@dataclass
class BearerAuthTokenManager:
    _registry: ClassVar[Dict[Any, BearerAuthTokenManager]] = {}

    def __init__(
        self,
        backend: Backend,
        callback: Callable[[], AuthTokenAndExpiration]
    ):
        self._backend = backend
        self._callback = callback

        req = NewBearerAuthTokenManager()
        res = backend.send_and_receive(req)
        if not isinstance(res, BearerAuthTokenManagerMessage):
            raise Exception(
                f"Should be BearerAuthTokenManager but was {res}"
            )

        self._bearer_auth_token_manager = res
        self._registry[self._bearer_auth_token_manager.id] = self

    @property
    def id(self):
        return self._bearer_auth_token_manager.id

    @classmethod
    def process_callbacks(cls, request):
        if isinstance(request,
                      BearerAuthTokenProviderRequest):
            if (
                request.bearer_auth_token_manager_id
                not in cls._registry
            ):
                raise Exception(
                    "Backend provided unknown BearerAuthTokenManager "
                    f"id: {request.bearer_auth_token_manager_id} "
                    f"not found"
                )

            manager = cls._registry[
                request.bearer_auth_token_manager_id
            ]
            renewable_auth_token = manager._callback()
            return BearerAuthTokenProviderCompleted(
                request.id, renewable_auth_token
            )

    def close(self, hooks=None):
        res = self._backend.send_and_receive(
            AuthTokenManagerClose(self.id),
            hooks=hooks
        )
        if not isinstance(res, AuthTokenManagerMessage):
            raise Exception(f"Should be AuthTokenManager but was {res}")
        del self._registry[self.id]
