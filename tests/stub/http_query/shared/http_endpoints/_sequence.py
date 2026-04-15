from __future__ import annotations

import re
import typing as t

from pytest_httpserver import RequestMatcher
from werkzeug import Response

from ._base import (
    HttpEndpoint,
    HttpEndpointStateful,
    TestKitRequestMatcher,
)

if t.TYPE_CHECKING:
    from werkzeug import Request

    from ..http_server import TestKitStubHttpServer

    _T_Handler: t.TypeAlias = t.Callable[[Request], Response]


class HttpSequenceEndpoint(HttpEndpointStateful):
    _endpoints: tuple[HttpEndpoint, ...]
    _matchers: tuple[RequestMatcher, ...]
    _handlers: tuple[_T_Handler, ...]
    _idx: int = 0

    def __init__(
        self,
        *endpoints: HttpEndpoint,
    ) -> None:
        self._endpoints = endpoints

    def _set_server(self, server: TestKitStubHttpServer) -> None:
        super()._set_server(server)
        endpoints = self._endpoints
        for endpoint in endpoints:
            endpoint._set_server(server)
        self._matchers = tuple(endpoint._matcher() for endpoint in endpoints)
        self._handlers = tuple(endpoint._handler() for endpoint in endpoints)

    def _matcher(self) -> RequestMatcher:
        class SequenceMatcher(TestKitRequestMatcher):
            def match(self, request: Request) -> bool:
                if this._idx >= len(this._matchers):
                    return False
                matcher = this._matchers[this._idx]
                return matcher.match(request)

            def __repr__(self) -> str:
                return (
                    f"<{self.__class__.__name__} {this._matchers!r} "
                    f"@{this._idx}>"
                )

            def pprint(self, prefix: str | None) -> str:
                prefix = prefix or ""
                top_state = " (DONE)" if this.done() else ""
                lines = [f"{prefix}{self.__class__.__name__}{top_state}:"]
                for i, matcher in enumerate(this._matchers):
                    matcher_prefix = "√ " if i < this._idx else "  "
                    lines.append(
                        TestKitRequestMatcher.format_matcher(
                            matcher, f"{prefix}{matcher_prefix}"
                        )
                    )
                return "\n".join(lines)

        this: HttpSequenceEndpoint = self

        return SequenceMatcher(re.compile(".*"))

    def _handler(self) -> t.Callable[[Request], Response]:
        def handler(request: Request) -> Response:
            if self._idx >= len(self._matchers):
                raise RuntimeError(
                    "Ran out of handlers; "
                    "this should not happen, because matcher shouldn't have "
                    "matched the request. "
                    f"Request: {request!r}, matchers: {self._matchers!r}"
                )

            handler = self._handlers[self._idx]
            endpoint = self._endpoints[self._idx]
            res = handler(request)
            if isinstance(endpoint, HttpEndpointStateful):
                if endpoint.done():
                    self._idx += 1
            else:
                self._idx += 1
            return res

        return handler

    def done(self) -> bool:
        return self._idx >= len(self._matchers)
