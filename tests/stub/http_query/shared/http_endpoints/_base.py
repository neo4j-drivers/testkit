from __future__ import annotations

import abc
import base64
import dataclasses
import typing as t
import urllib.parse

import typing_extensions as te
from pytest_httpserver.httpserver import (
    HandlerType,
    RequestMatcher,
)

from ..http_types import (
    HttpType,
    ProtocolVersion,
)

if t.TYPE_CHECKING:
    from werkzeug import (
        Request,
        Response,
    )
    from werkzeug.datastructures import Headers

    from nutkit import protocol as types

    from ..http_server import TestKitStubHttpServer

T = t.TypeVar("T")


__all__: tuple[str, ...] = (
    "AnyValue",
    "AutoRespond",
    "CountersMap",
    "CustomAuthToken",
    "HttpEndpoint",
    "MaybeNull",
    "Plan",
    "Position",
    "Profile",
    "ProtocolVersion",
    "serialize_any",
    "TestKitRequestMatcher",
    "url_encode",
)


@dataclasses.dataclass
class MaybeNull(t.Generic[T]):
    """
    Instruct a matcher to accept requests with this value missing/bein None.

    E.g., ``MaybeNull({})`` is supposed to match json values ``null``, ``{}``,
    and omission of the key-value if inside an object.
    """

    value: T


if t.TYPE_CHECKING:
    TOptionalValue: t.TypeAlias = T | MaybeNull[T]

    __all__ += ("TOptionalValue",)


@dataclasses.dataclass
class AnyValue:
    pass


@dataclasses.dataclass
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


@dataclasses.dataclass
class Plan:
    identifiers: list[str] = dataclasses.field(default_factory=list)
    operator_type: str = "ProduceResults@neo4j"
    arguments: dict[str, HttpType] = dataclasses.field(default_factory=dict)
    children: list[Plan] = dataclasses.field(default_factory=list)

    def json_dict(
        self,
        protocol_version: ProtocolVersion,
    ) -> dict[str, object]:
        return {
            "identifiers": self.identifiers,
            "operatorType": self.operator_type,
            "arguments": {
                k: v.serialize(protocol_version)
                for k, v in self.arguments.items()
            },
            "children": [
                child.json_dict(protocol_version) for child in self.children
            ],
        }


@dataclasses.dataclass
class Profile:
    db_hits: int = 0
    records: int = 0
    has_page_cache_stats: bool = False
    page_cache_hits: int = 0
    page_cache_misses: int = 0
    page_cache_hit_ratio: float = 0.0
    time: int = 0
    identifiers: list[str] = dataclasses.field(default_factory=list)
    operator_type: str = "ProduceResults@neo4j"
    arguments: dict[str, HttpType] = dataclasses.field(default_factory=dict)
    children: list[Profile] = dataclasses.field(default_factory=list)

    def json_dict(
        self,
        protocol_version: ProtocolVersion,
    ) -> dict[str, object]:
        return {
            "dbHits": self.db_hits,
            "records": self.records,
            "hasPageCacheStats": self.has_page_cache_stats,
            "pageCacheHits": self.page_cache_hits,
            "pageCacheMisses": self.page_cache_misses,
            "pageCacheHitRatio": self.page_cache_hit_ratio,
            "time": self.time,
            "identifiers": self.identifiers,
            "operatorType": self.operator_type,
            "arguments": {
                k: v.serialize(protocol_version)
                for k, v in self.arguments.items()
            },
            "children": [
                child.json_dict(protocol_version) for child in self.children
            ],
        }


@dataclasses.dataclass
class Position:
    offset: int = -1
    line: int = -1
    column: int = -1

    def json_dict(self) -> dict[str, object]:
        return {
            "offset": self.offset,
            "line": self.line,
            "column": self.column,
        }


@dataclasses.dataclass
class Notification:
    code: str | None = "Neo.DatabaseError.General.UnknownError"
    description: str | None = "An unknown error occurred"
    severity: str = "N/A"
    title: str | None = "UnknownError"
    position: Position | None = dataclasses.field(default_factory=Position)
    category: str | None = None

    def json_dict(self) -> dict[str, object]:
        position = None if self.position is None else self.position.json_dict()
        result: dict[str, object] = {
            "code": self.code,
            "description": self.description,
            "severity": self.severity,
            "title": self.title,
            "position": position,
        }
        if self.category is not None:
            result["category"] = self.category
        return result


class TestKitRequestMatcher(RequestMatcher, abc.ABC):
    @abc.abstractmethod
    def pprint(self, prefix: str | None) -> str: ...

    @staticmethod
    def format_matcher(handler: RequestMatcher, prefix: str | None) -> str:
        if isinstance(handler, TestKitRequestMatcher):
            return handler.pprint(prefix)
        return f"{prefix or ''}{handler!r}"


class HttpEndpoint(abc.ABC):
    _server: TestKitStubHttpServer

    def _set_server(self, server: TestKitStubHttpServer) -> None:
        """Must be called before calling _matcher() or __handler()."""
        self._server = server

    @abc.abstractmethod
    def _matcher(self) -> RequestMatcher: ...

    @abc.abstractmethod
    def _handler(self) -> t.Callable[[Request], Response]: ...

    def install(
        self,
        server: TestKitStubHttpServer,
        handler_type: HandlerType = HandlerType.ORDERED,
    ) -> None:
        self._set_server(server)
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


@dataclasses.dataclass
class CustomAuthToken:
    expected_headers: dict[str, str] = dataclasses.field(
        default_factory=dict, kw_only=True
    )


def is_str_dict(value: object) -> te.TypeGuard[dict[str, object]]:
    return isinstance(value, dict) and all(isinstance(k, str) for k in value)


def serialize_any(value: object, protocol_version: ProtocolVersion) -> object:
    if isinstance(value, HttpType):
        return value.serialize(protocol_version)
    elif isinstance(value, list):
        return [serialize_any(v, protocol_version) for v in value]
    elif isinstance(value, dict):
        return {
            k: serialize_any(v, protocol_version) for k, v in value.items()
        }
    else:
        return value


def url_encode(s: str) -> str:
    return urllib.parse.quote(s, safe="")
