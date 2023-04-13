from __future__ import annotations

from dataclasses import dataclass
from typing import (
    Any,
    Callable,
    ClassVar,
    Dict,
)

from .. import protocol
from ..backend import Backend


@dataclass
class AuthTokenManager:
    _registry: ClassVar[Dict[Any, AuthTokenManager]] = {}

    def __init__(
        self,
        backend: Backend,
        get_auth: Callable[[], protocol.AuthorizationToken],
        on_auth_expired: Callable[[protocol.AuthorizationToken], None]
    ):
        self._backend = backend
        self._get_auth = get_auth
        self._on_auth_expired = on_auth_expired

        req = protocol.NewAuthTokenManager()
        res = backend.send_and_receive(req)
        if not isinstance(res, protocol.AuthTokenManager):
            raise Exception(f"Should be AuthTokenManager but was {res}")

        self._auth_token_manager = res
        self._registry[self._auth_token_manager.id] = self

    @property
    def id(self):
        return self._auth_token_manager.id

    @classmethod
    def process_callbacks(cls, request):
        if isinstance(request, protocol.AuthTokenManagerGetAuthRequest):
            if request.auth_token_manager_id not in cls._registry:
                raise Exception(
                    "Backend provided unknown Auth Token Manager "
                    f"id: {request.auth_token_manager_id} not found"
                )
            manager = cls._registry[request.auth_token_manager_id]
            auth_token = manager._get_auth()
            return protocol.AuthTokenManagerGetAuthCompleted(
                request.id, auth_token
            )
        if isinstance(request, protocol.AuthTokenManagerOnAuthExpiredRequest):
            if request.auth_token_manager_id not in cls._registry:
                raise Exception(
                    "Backend provided unknown Auth Token Manager "
                    f"id: {request.auth_token_manager_id} not found"
                )
            manager = cls._registry[request.auth_token_manager_id]
            manager._on_auth_expired(request.auth)
            return protocol.AuthTokenManagerOnAuthExpiredCompleted(request.id)

    def close(self, hooks=None):
        res = self._backend.send_and_receive(
            protocol.AuthTokenManagerClose(self.id),
            hooks=hooks
        )
        if not isinstance(res, protocol.AuthTokenManager):
            raise Exception(
                f"Should be AuthTokenManager but was {res}"
            )
        del self._registry[self.id]


@dataclass
class ExpirationBasedAuthTokenManager:
    _registry: ClassVar[Dict[Any, ExpirationBasedAuthTokenManager]] = {}

    def __init__(
        self,
        backend: Backend,
        callback: Callable[[], protocol.AuthTokenAndExpiration]
    ):
        self._backend = backend
        self._callback = callback

        req = protocol.NewExpirationBasedAuthTokenManager()
        res = backend.send_and_receive(req)
        if not isinstance(res, protocol.ExpirationBasedAuthTokenManager):
            raise Exception(
                f"Should be TemporalAuthTokenManager but was {res}"
            )

        self._temporal_auth_token_manager = res
        self._registry[self._temporal_auth_token_manager.id] = self

    @property
    def id(self):
        return self._temporal_auth_token_manager.id

    @classmethod
    def process_callbacks(cls, request):
        if isinstance(request,
                      protocol.ExpirationBasedAuthTokenProviderRequest):
            if (
                request.expiration_based_auth_token_manager_id
                not in cls._registry
            ):
                raise Exception(
                    "Backend provided unknown ExpirationBasedAuthTokenManager "
                    f"id: {request.expiration_based_auth_token_manager_id} "
                    f"not found"
                )

            manager = cls._registry[
                request.expiration_based_auth_token_manager_id
            ]
            renewable_auth_token = manager._callback()
            return protocol.ExpirationBasedAuthTokenProviderCompleted(
                request.id, renewable_auth_token
            )

    def close(self, hooks=None):
        res = self._backend.send_and_receive(
            protocol.AuthTokenManagerClose(self.id),
            hooks=hooks
        )
        if not isinstance(res, protocol.AuthTokenManager):
            raise Exception(f"Should be AuthTokenManager but was {res}")
        del self._registry[self.id]
