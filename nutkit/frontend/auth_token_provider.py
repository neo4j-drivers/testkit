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
class AuthTokenProvider:
    _registry: ClassVar[Dict[Any, AuthTokenProvider]] = {}

    def __init__(self,
                 backend: Backend,
                 callback: Callable[[], protocol.RenewableAuthToken]):
        self._backend = backend
        self.callback = callback

        req = protocol.NewAuthTokenProvider()
        res = backend.send_and_receive(req)
        if not isinstance(res, protocol.AuthTokenProvider):
            raise Exception(f"Should be AuthTokenProvider but was {res}")

        self._auth_token_provider = res
        self._registry[self._auth_token_provider.id] = self

    @property
    def id(self):
        return self._auth_token_provider.id

    @classmethod
    def process_callbacks(cls, request):
        if isinstance(request, protocol.AuthTokenProviderRequest):
            if request.auth_token_provider_id not in cls._registry:
                raise Exception(
                    "Backend provided unknown Auth Token Provider "
                    f"id: {request.auth_token_provider_id} not found"
                )

            provider = cls._registry[request.auth_token_provider_id]
            renewable_auth_token = provider.callback()
            return protocol.AuthTokenProviderCompleted(
                request.id, renewable_auth_token
            )

    def close(self, hooks=None):
        res = self._backend.send_and_receive(
            protocol.AuthTokenProviderClose(self.id),
            hooks=hooks
        )
        if not isinstance(res, protocol.AuthTokenProvider):
            raise Exception(f"Should be AuthTokenProvider but was {res}")
        del self._registry[self.id]
