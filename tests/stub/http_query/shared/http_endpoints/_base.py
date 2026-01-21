from __future__ import annotations

import abc
import base64
import dataclasses
import typing as t

import typing_extensions as te
from pytest_httpserver.httpserver import HandlerType

from ..http_types import ProtocolVersion

if t.TYPE_CHECKING:
    from pytest_httpserver import (
        HTTPServer,
        RequestMatcher,
    )
    from werkzeug import (
        Request,
        Response,
    )
    from werkzeug.datastructures import Headers

    from nutkit import protocol as types

T = t.TypeVar("T")


__all__: tuple[str, ...] = (
    "AnyValue",
    "AutoRespond",
    "CountersMap",
    "CustomAuthToken",
    "HttpEndpoint",
    "MaybeNull",
    "ProtocolVersion",
)


@dataclasses.dataclass(frozen=True)
class MaybeNull(t.Generic[T]):
    value: T


if t.TYPE_CHECKING:
    TOptionalValue: t.TypeAlias = T | MaybeNull[T]

    __all__ += ("TOptionalValue",)


@dataclasses.dataclass(frozen=True)
class AnyValue:
    pass


@dataclasses.dataclass(frozen=True)
class AutoRespond:
    pass


@dataclasses.dataclass
class CountersMap:
    contains_updates: int = True
    nodes_created: int = 0
    nodes_deleted: int = 0
    properties_set: int = 0
    relationships_created: int = 0
    relationships_deleted: int = 0
    labels_added: int = 0
    labels_removed: int = 0
    indexes_added: int = 0
    indexes_removed: int = 0
    constraints_added: int = 0
    constraints_removed: int = 0
    contains_system_updates: int = False
    system_updates: int = 0

    def json_dict(self) -> dict[str, object]:
        return {
            "containsUpdates": self.contains_updates,
            "nodesCreated": self.nodes_created,
            "nodesDeleted": self.nodes_deleted,
            "propertiesSet": self.properties_set,
            "relationshipsCreated": self.relationships_created,
            "relationshipsDeleted": self.relationships_deleted,
            "labelsAdded": self.labels_added,
            "labelsRemoved": self.labels_removed,
            "indexesAdded": self.indexes_added,
            "indexesRemoved": self.indexes_removed,
            "constraintsAdded": self.constraints_added,
            "constraintsRemoved": self.constraints_removed,
            "containsSystemUpdates": self.contains_system_updates,
            "systemUpdates": self.system_updates,
        }


class HttpEndpoint(abc.ABC):
    @abc.abstractmethod
    def _matcher(self) -> RequestMatcher: ...

    @abc.abstractmethod
    def _handler(self) -> t.Callable[[Request], Response]: ...

    def install(
        self,
        server: HTTPServer,
        handler_type: HandlerType = HandlerType.ORDERED,
    ) -> None:
        handler = server.expect(
            self._matcher(),
            handler_type=handler_type,
        )
        handler.respond_with_handler(self._handler())

    @staticmethod
    def _auth_to_header(
        auth: types.AuthorizationToken | CustomAuthToken,
    ) -> dict[str, str]:
        if isinstance(auth, CustomAuthToken):
            return auth.expected_headers
        if auth.scheme == "basic":
            username = auth.principal  # type: ignore
            password = auth.credentials  # type: ignore
            if getattr(auth, "realm", None) is not None:
                raise ValueError(
                    "Basic auth `realm` is not supported via HTTP"
                )
            auth_bytes = f"{username}:{password}".encode("utf-8")
            b64_auth_bytes = base64.b64encode(auth_bytes).decode("ascii")
            return {"Authorization": f"Basic {b64_auth_bytes}"}
        if auth.scheme == "bearer":
            token = auth.credentials  # type: ignore
            token_bytes = token.encode("utf-8")
            b64_token_bytes = base64.b64encode(token_bytes).decode("ascii")
            return {"Authorization": f"Bearer {b64_token_bytes}"}

        raise ValueError(
            f"Auth scheme {auth.scheme} is not supported via HTTP. "
            "Available schemes: basic, bearer"
        )

    @classmethod
    def _verify_version_header(
        cls,
        protocol_version: ProtocolVersion,
        headers: Headers,
    ) -> bool:
        content_type = headers.get("Content-Type")
        if content_type is None:
            return False
        if protocol_version == ProtocolVersion.V1_0:
            return content_type in {
                "application/vnd.neo4j.query",
                "application/vnd.neo4j.query.v1.0",
            }
        elif protocol_version == ProtocolVersion.V1_1:
            return content_type == "application/vnd.neo4j.query.v1.1"
        else:
            NotImplementedError("TODO")

    @classmethod
    def _version_as_header(
        cls,
        protocol_version: ProtocolVersion,
    ) -> dict[str, str]:
        if protocol_version == ProtocolVersion.V1_0:
            return {
                "Content-Type": "application/vnd.neo4j.query",
            }
        elif protocol_version == ProtocolVersion.V1_1:
            return {
                "Content-Type": "application/vnd.neo4j.query.v1.1",
            }
        else:
            NotImplementedError("TODO")


class HttpEndpointStateful(HttpEndpoint, abc.ABC):
    @abc.abstractmethod
    def done(self) -> bool: ...


@dataclasses.dataclass(frozen=True)
class CustomAuthToken:
    expected_headers: dict[str, str] = dataclasses.field(
        default_factory=dict, kw_only=True
    )


def is_str_dict(value: object) -> te.TypeGuard[dict[str, object]]:
    return isinstance(value, dict) and all(isinstance(k, str) for k in value)
