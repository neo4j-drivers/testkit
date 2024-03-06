from __future__ import annotations

from dataclasses import dataclass
from typing import (
    Any,
    Callable,
    ClassVar,
    Dict,
)

from ..backend import Backend
from ..protocol import ClientCertificate
from ..protocol import (
    ClientCertificateProvider as ClientCertificateProviderMessage,
)
from ..protocol import (
    ClientCertificateProviderClose,
    ClientCertificateProviderCompleted,
    ClientCertificateProviderRequest,
    NewClientCertificateProvider,
)

__all__ = [
    "ClientCertificateHolder",
    "ClientCertificateProvider",
]


@dataclass
class ClientCertificateHolder:
    cert: ClientCertificate
    has_update: bool = True


class ClientCertificateProvider:
    _registry: ClassVar[Dict[Any, ClientCertificateProvider]] = {}
    _backend: Any
    _handler: Callable[[], ClientCertificateHolder]

    def __init__(
        self,
        backend: Backend,
        handler: Callable[[], ClientCertificateHolder],
    ):
        self._backend = backend
        self._handler = handler

        req = NewClientCertificateProvider()
        res = backend.send_and_receive(req)
        if not isinstance(res, ClientCertificateProviderMessage):
            raise Exception(
                f"Should be ClientCertificateProvider but was {res}"
            )

        self._client_certificate_provider = res
        self._registry[self._client_certificate_provider.id] = self

    @property
    def id(self):
        return self._client_certificate_provider.id

    @classmethod
    def process_callbacks(cls, request):
        if isinstance(request, ClientCertificateProviderRequest):
            if request.client_certificate_provider_id not in cls._registry:
                raise Exception(
                    "Backend provided unknown Client Certificate Provider "
                    f"id: {request.client_certificate_provider_id} not found"
                )
            manager = cls._registry[request.client_certificate_provider_id]
            cert_holder = manager._handler()
            return ClientCertificateProviderCompleted(
                request.id, cert_holder.has_update, cert_holder.cert
            )

    def close(self, hooks=None):
        res = self._backend.send_and_receive(
            ClientCertificateProviderClose(self.id),
            hooks=hooks
        )
        if not isinstance(res, ClientCertificateProviderMessage):
            raise Exception(
                f"Should be ClientCertificateProvider but was {res}"
            )
        del self._registry[self.id]
