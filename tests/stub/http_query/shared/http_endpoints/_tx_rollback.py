from __future__ import annotations

import json
import re
import typing as t
from dataclasses import dataclass

from pytest_httpserver import RequestMatcher
from werkzeug import Response

from ..http_types import ProtocolVersion
from ._base import (
    CustomAuthToken,
    HttpEndpoint,
    url_encode,
)

if t.TYPE_CHECKING:
    from werkzeug import Request
    from werkzeug.datastructures import Headers

    from nutkit import protocol as types


class HttpTxRollbackEndpoint(HttpEndpoint):
    @dataclass
    class RequestData:
        db: str | re.Pattern[str]
        tx_id: str | re.Pattern[str]
        auth: types.AuthorizationToken | CustomAuthToken

    _req: RequestData
    _protocol_version: ProtocolVersion
    _extra_body_verification: tuple[t.Callable[[dict[str, object]], bool], ...]
    _extra_header_verification: tuple[t.Callable[[Headers], bool], ...]
    _legacy_response: bool

    def __init__(
        self,
        request: RequestData,
        protocol_version: ProtocolVersion = ProtocolVersion.V1_0,
        extra_body_verification: t.Iterable[t.Callable[[object], bool]] = (),
        extra_header_verification: t.Iterable[
            t.Callable[[Headers], bool]
        ] = (),
        # Some older servers (e.g., 2025.11) respond with an empty body and no
        # Content-Type header set.
        legacy_response: bool = False,
    ) -> None:
        self._req = request
        self._protocol_version = protocol_version
        self._extra_body_verification = tuple(extra_body_verification)
        self._extra_header_verification = tuple(extra_header_verification)
        self._legacy_response = legacy_response

    def _matcher(self) -> RequestMatcher:
        class TxRollbackMatcher(RequestMatcher):
            def match(self, request: Request) -> bool:
                match = super().match(request)
                if not match:
                    return match

                headers: Headers = request.headers
                if not match_headers(headers):
                    return False

                return True

        def match_headers(headers: Headers) -> bool:
            if not all(
                check(headers) for check in self._extra_header_verification
            ):
                return False
            return True

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
            tx_id = self._req.tx_id.pattern
        else:
            tx_id = url_encode(self._req.tx_id)
            if url_re:
                tx_id = re.escape(tx_id)
        url: str | re.Pattern[str] = f"/db/{db}/query/v2/tx/{tx_id}"
        if url_re:
            url = re.compile(f"^{url}$")

        return TxRollbackMatcher(
            url,
            method="DELETE",
            headers=self._auth_to_header(self._req.auth),
        )

    def _handler(self) -> t.Callable[[Request], Response]:
        if self._legacy_response:
            return self._legacy_handler()

        def handler(req: Request) -> Response:
            body: dict[str, t.Any] = {}
            return Response(
                json.dumps(body),
                status=200,
                headers=self._version_as_header(self._protocol_version),
            )

        return handler

    def _legacy_handler(self) -> t.Callable[[Request], Response]:
        def handler(req: Request) -> Response:
            return Response(b"", status=200)

        return handler
