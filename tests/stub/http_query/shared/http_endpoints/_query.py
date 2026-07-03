from __future__ import annotations

import json
import re
import typing as t
from dataclasses import (
    dataclass,
    field,
)

from pytest_httpserver import RequestMatcher
from werkzeug import Response

from ..http_types import (
    HttpType,
    ProtocolVersion,
)
from ._base import (
    AnyValue,
    AutoRespond,
    CountersMap,
    CustomAuthToken,
    HttpEndpoint,
    is_str_dict,
    MaybeNull,
    Notification,
    Plan,
    Profile,
    serialize_any,
    url_encode,
)

if t.TYPE_CHECKING:
    from werkzeug import Request
    from werkzeug.datastructures import Headers

    from nutkit import protocol as types

    from ._base import TOptionalValue


class HttpQueryEndpoint(HttpEndpoint):
    @dataclass
    class RequestData:
        db: str | re.Pattern[str]
        auth: types.AuthorizationToken | CustomAuthToken
        query: str | re.Pattern[str]
        impersonated_user: str | re.Pattern[str] | None = None
        access_mode: TOptionalValue[str] | AnyValue | None = field(
            default_factory=AnyValue
        )
        parameters: TOptionalValue[dict[str, HttpType]] | None = field(
            default_factory=lambda: MaybeNull(t.cast(dict[str, HttpType], {}))
        )
        bookmarks: list[str] = field(default_factory=list)
        include_counters: TOptionalValue[bool] | AnyValue | None = field(
            default_factory=AnyValue
        )

        def _match_query(self, query: object) -> bool:
            return self._match_str_or_pattern(query, self.query, "query")

        def _match_impersonated_user(self, impersonated_user: object) -> bool:
            return self._match_str_or_pattern(
                impersonated_user, self.impersonated_user, "impersonated_user"
            )

        def _match_access_mode(self, access_mode: object) -> bool:
            expected = self.access_mode
            if isinstance(expected, AnyValue):
                return True
            if expected is None:
                return access_mode is None
            if isinstance(expected, MaybeNull):
                if access_mode is None:
                    return True
                expected = expected.value
            if not isinstance(access_mode, str):
                return False
            return access_mode == expected

        def _match_parameters(
            self, parameters: object, protocol_version: ProtocolVersion
        ) -> bool:
            expected = self.parameters
            if expected is None:
                return parameters is None
            if isinstance(expected, MaybeNull):
                if parameters is None:
                    return True
                expected = expected.value
            if not is_str_dict(parameters):
                return False
            if len(parameters) != len(expected):
                return False
            if parameters.keys() != expected.keys():
                return False
            for k, v in parameters.items():
                parsed_v = HttpType.deserialize(v, protocol_version)
                if expected[k] != parsed_v:
                    return False
            return True

        def _match_bookmarks(self, bookmarks: object) -> bool:
            if self.bookmarks == [] and bookmarks is None:
                return True
            if not isinstance(bookmarks, list):
                return False
            if len(bookmarks) != len(self.bookmarks):
                return False
            return sorted(bookmarks) == sorted(self.bookmarks)

        def _match_include_counters(self, include_counters: object) -> bool:
            expected = self.include_counters
            if isinstance(expected, AnyValue):
                return True
            if expected is None:
                return include_counters is None
            if isinstance(expected, MaybeNull):
                if include_counters is None:
                    return True
                expected = expected.value
            if not isinstance(include_counters, bool):
                return False
            return include_counters == expected

        @staticmethod
        def _match_str_or_pattern(
            received: object, expected: object, name: str
        ) -> bool:
            if expected is None:
                return received is None
            if not isinstance(received, str):
                return False
            if isinstance(expected, str):
                return received == expected
            if isinstance(expected, re.Pattern):
                return expected.match(received) is not None
            typ = type(expected)
            raise TypeError(f"Unsupported {name} match type {typ}")

    @dataclass
    class ResponseData:
        fields: list[str] | None = None
        records: list[list[HttpType]] | None = None
        bookmarks: list[str] | None = None
        counters: CountersMap | AutoRespond | None = field(
            default_factory=AutoRespond
        )
        result_available_after: int | None = None
        result_consumed_after: int | None = None
        plan: Plan | None = None
        profile: Profile | None = None
        notifications: list[Notification] | None = None
        errors: list[dict[str, object]] | None = None
        status_code: int | None = None

        def _get_counters(self, req: Request) -> t.Any:
            if isinstance(self.counters, AutoRespond):
                body = req.get_json(force=True)
                if body.get("include_counters") is True:
                    return CountersMap().json_dict()
                return None

            if isinstance(self.counters, CountersMap):
                return self.counters.json_dict()
            else:
                return self.counters

    _req: RequestData
    _res: ResponseData
    _protocol_version: ProtocolVersion
    _extra_body_verification: tuple[t.Callable[[dict[str, object]], bool], ...]
    _extra_header_verification: tuple[t.Callable[[Headers], bool], ...]

    def __init__(
        self,
        request: RequestData,
        response: ResponseData,
        protocol_version: ProtocolVersion = ProtocolVersion.V1_0,
        extra_body_verification: t.Iterable[
            t.Callable[[dict[str, object]], bool]
        ] = (),
        extra_header_verification: t.Iterable[
            t.Callable[[Headers], bool]
        ] = (),
    ) -> None:
        self._req = request
        self._res = response
        self._protocol_version = protocol_version
        self._extra_body_verification = tuple(extra_body_verification)
        self._extra_header_verification = tuple(extra_header_verification)

    def _matcher(self) -> RequestMatcher:
        class QueryMatcher(RequestMatcher):
            def match(self, request: Request) -> bool:
                match = super().match(request)
                if not match:
                    return match

                headers: Headers = request.headers
                if not match_headers(headers):
                    return False

                body = request.get_json(force=True)
                if not match_body(body):
                    return False

                return True

            def __repr__(self) -> str:
                super_repr = super().__repr__()

                fields = [
                    f"{super_repr[:-1]}",
                    f"protocol_version={this._protocol_version!r}",
                    f"query={this._req.query!r}",
                ]
                if this._req.impersonated_user is not None:
                    fields += (
                        f"impersonated_user={this._req.impersonated_user!r}"
                    )
                if not isinstance(this._req.access_mode, AnyValue):
                    fields.append(f"access_mode={this._req.access_mode!r}")
                fields.append(f"bookmarks={this._req.bookmarks!r}")
                if not isinstance(this._req.include_counters, AnyValue):
                    fields.append(
                        f"include_counters={this._req.include_counters!r}"
                    )
                fields.append(f"parameters={this._req.parameters!r}")

                return f"{' '.join(fields)}>"

        def match_headers(headers: Headers) -> bool:
            if not self._verify_version_header(
                self._protocol_version, headers
            ):
                return False
            if not all(
                check(headers) for check in self._extra_header_verification
            ):
                return False
            return True

        def match_body(body: object) -> bool:
            if not is_str_dict(body):
                return False
            statement = body.get("statement")
            if not self._req._match_query(statement):
                return False
            impersonated_user = body.get("impersonatedUser")
            if not self._req._match_impersonated_user(impersonated_user):
                return False
            access_mode = body.get("accessMode")
            if not self._req._match_access_mode(access_mode):
                return False
            bookmarks = body.get("bookmarks")
            if not self._req._match_bookmarks(bookmarks):
                return False
            include_counters = body.get("includeCounters")
            if not self._req._match_include_counters(include_counters):
                return False
            parameters = body.get("parameters")
            if not self._req._match_parameters(
                parameters, self._protocol_version
            ):
                return False
            if not all(check(body) for check in this._extra_body_verification):
                return False
            return True

        this: HttpQueryEndpoint = self

        url: str | re.Pattern[str]
        if isinstance(self._req.db, str):
            url = self._server.prefix_path(
                f"/db/{url_encode(self._req.db)}/query/v2"
            )
        else:
            url = re.compile(
                self._server.prefix_path(
                    f"/db/{self._req.db.pattern}/query/v2"
                )
            )

        return QueryMatcher(
            url,
            method="POST",
            headers=self._auth_to_header(self._req.auth),
        )

    def _handler(self) -> t.Callable[[Request], Response]:
        def handler(req: Request) -> Response:
            body: dict[str, t.Any] = {}

            data: dict[str, t.Any] = {}
            if self._res.fields is not None:
                data["fields"] = self._res.fields
            if self._res.records is not None:
                data["values"] = tuple(
                    tuple(
                        value.serialize(self._protocol_version)
                        for value in record
                    )
                    for record in self._res.records
                )
            if data:
                body["data"] = data

            if self._res.bookmarks is not None:
                body["bookmarks"] = self._res.bookmarks

            counters = self._res._get_counters(req)
            if counters is not None:
                body["counters"] = counters

            if self._res.result_available_after is not None:
                body["resultAvailableAfter"] = self._res.result_available_after
            if self._res.result_consumed_after is not None:
                body["resultConsumedAfter"] = self._res.result_consumed_after

            if self._res.plan is not None:
                body["queryPlan"] = self._res.plan.json_dict(
                    self._protocol_version
                )

            if self._res.profile is not None:
                body["profiledQueryPlan"] = self._res.profile.json_dict(
                    self._protocol_version
                )

            if self._res.notifications is not None:
                body["notifications"] = [
                    notification.json_dict()
                    for notification in self._res.notifications
                ]

            if self._res.errors is not None:
                body["errors"] = [
                    {
                        k: serialize_any(v, self._protocol_version)
                        for k, v in error.items()
                    }
                    for error in self._res.errors
                ]

            if self._res.status_code is None:
                status_code = 400 if self._res.errors else 202
            else:
                status_code = self._res.status_code

            return Response(
                json.dumps(body),
                status=status_code,
                headers=self._version_as_header(self._protocol_version),
            )

        return handler
