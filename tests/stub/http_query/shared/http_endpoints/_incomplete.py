from __future__ import annotations

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


class HttpIncompleteEndpoint(HttpEndpointStateful):
    """
    Wrap any ``HttpEndpoint`` simulating a network abort.

    The response body of the wrapped endpoint is being replaced with an empty
    body and the Content-Length set to > 0. This is equivalent to the
    connection being closed (e.g., by an intermediate load-balancer) right
    after the headers have been transmitted (but before the body).
    """

    _inner: HttpEndpoint
    _done: bool

    def __init__(
        self,
        endpoint: HttpEndpoint,
    ) -> None:
        self._inner = endpoint

    def _set_server(self, server: TestKitStubHttpServer) -> None:
        super()._set_server(server)
        self._inner._set_server(server)

    def _matcher(self) -> RequestMatcher:
        class IncompleteMatcher(TestKitRequestMatcher):
            def match(self, request: Request) -> bool:
                return inner_matcher.match(request)

            def __repr__(self) -> str:
                return f"<{self.__class__.__name__} {inner_matcher!r}>"

            def pprint(self, prefix: str | None) -> str:
                prefix = prefix or ""
                top_state = " (DONE)" if this.done() else ""
                inner_pprint = TestKitRequestMatcher.format_matcher(
                    inner_matcher, f"{prefix}  "
                )

                return (
                    f"{prefix}{self.__class__.__name__}{top_state}:\n"
                    f"{inner_pprint}"
                )

        this = self
        inner_matcher = self._inner._matcher()
        return IncompleteMatcher(inner_matcher.uri)

    def _handler(self) -> t.Callable[[Request], Response]:
        inner_handler = self._inner._handler()

        def handler(request: Request) -> Response:
            response = inner_handler(request)
            response.response = [b""]
            response.headers["Content-Length"] = "1"
            if isinstance(self._inner, HttpEndpointStateful):
                self._done = self._inner.done()
            else:
                self._done = True

            return response

        return handler

    def done(self) -> bool:
        return self._done
