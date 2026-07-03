from __future__ import annotations

import json
import re
import typing as t
from dataclasses import (
    dataclass,
    field,
)
from datetime import (
    datetime,
    timedelta,
    UTC,
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


class HttpTxQueryEndpoint(HttpEndpoint):
    @dataclass
    class RequestData:
        db: str | re.Pattern[str]
        tx_id: str | re.Pattern[str]
        auth: types.AuthorizationToken | CustomAuthToken
        query: str | re.Pattern[str]
        parameters: TOptionalValue[dict[str, HttpType]] | None = field(
            default_factory=lambda: MaybeNull(t.cast(dict[str, HttpType], {}))
        )
        include_counters: TOptionalValue[bool] | AnyValue | None = field(
            default_factory=AnyValue
        )

        def _match_query(self, query: object) -> bool:
            if not isinstance(query, str):
                return False
            if isinstance(self.query, str):
                return query == self.query
            if isinstance(self.query, re.Pattern):
                return self.query.match(query) is not None
            raise TypeError(f"Unsupported query match type {type(self.query)}")

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

    @dataclass
    class ResponseData:
        transaction: Tx | None
        fields: list[str] | None = None
        records: list[list[HttpType]] | None = None
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

        @dataclass
        class Tx:
            id: str | None = None  # if none, will be taken from RequestData
            expires: str = field(
                default_factory=lambda: (
                    datetime.now(UTC) + timedelta(hours=1)
                ).strftime("%Y-%m-%dT%H:%M:%SZ")
            )

        def _get_counters(self, req: Request) -> t.Any:
            if isinstance(self.counters, AutoRespond):
                body = req.get_json(force=True)
                if body.get("include_counters") is True and isinstance(
                    body.get("query"), str
                ):
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
        class TxQueryMatcher(RequestMatcher):
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
                super_repr = super().__repr__()

                fields = [
                    f"{super_repr[:-1]}",
                    f"protocol_version={this._protocol_version!r}",
                    f"query={this._req.query!r}",
                ]
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

        this: HttpTxQueryEndpoint = self

        db_re = not isinstance(self._req.db, str)
        tx_id_re = not isinstance(self._req.tx_id, str)
        url_re = db_re or tx_id_re
        if not isinstance(self._req.db, str):
            db = self._req.db.pattern
        else:
            db = url_encode(self._req.db)
            if url_re:
                db = re.escape(db)
        if not isinstance(self._req.tx_id, str):
            if (
                self._res.transaction is not None
                and self._res.transaction.id is None
            ):
                raise ValueError(
                    "If request tx_id is a regex, responses transaction id "
                    "must be specified"
                )
            tx_id = self._req.tx_id.pattern
        else:
            tx_id = url_encode(self._req.tx_id)
            if url_re:
                tx_id = re.escape(tx_id)
        url: str | re.Pattern[str] = self._server.prefix_path(
            f"/db/{db}/query/v2/tx/{tx_id}"
        )
        if url_re:
            url = re.compile(f"^{url}$")

        return TxQueryMatcher(
            url,
            method="POST",
            headers=self._auth_to_header(self._req.auth),
        )

    def _handler(self) -> t.Callable[[Request], Response]:
        def handler(req: Request) -> Response:
            body: dict[str, t.Any] = {}

            if self._res.transaction is not None:
                tx_id = self._res.transaction.id
                if tx_id is None:
                    assert isinstance(self._req.tx_id, str)
                    tx_id = self._req.tx_id
                transaction: dict[str, t.Any] = {"id": tx_id}
                if self._res.transaction.expires is not None:
                    transaction["expires"] = self._res.transaction.expires
                body["transaction"] = transaction

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
            body["data"] = data

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
