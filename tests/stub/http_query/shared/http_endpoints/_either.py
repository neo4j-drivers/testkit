from __future__ import annotations

import re
import typing as t

from pytest_httpserver import RequestMatcher
from werkzeug import Response

from ._base import (
    HttpEndpoint,
    HttpEndpointStateful,
)

if t.TYPE_CHECKING:
    from werkzeug import Request

    _T_Handler: t.TypeAlias = t.Callable[[Request], Response]


class HttpEitherEndpoint(HttpEndpointStateful):
    _endpoints: tuple[HttpEndpoint, ...]
    _matchers: tuple[RequestMatcher, ...]
    _handlers: tuple[_T_Handler, ...]
    _selected: int | None = None
    _done: bool = False
    _last_request: Request | None = None
    _last_request_endpoint_id: int | None = None

    def __init__(
        self,
        *endpoints: HttpEndpoint,
    ) -> None:
        self._endpoints = endpoints
        self._matchers = tuple(endpoint._matcher() for endpoint in endpoints)
        self._handlers = tuple(endpoint._handler() for endpoint in endpoints)

    def _get_endpoint_idx(self, request: Request) -> int | None:
        if request is self._last_request:
            return self._last_request_endpoint_id
        assert (
            len(self._matchers) == len(self._handlers) == len(self._endpoints)
        )
        result = None
        for idx in range(len(self._endpoints)):
            matcher = self._matchers[idx]
            if matcher.match(request):
                result = idx
                break
        self._last_request = request
        self._last_request_endpoint_id = result
        return result

    def _matcher(self) -> RequestMatcher:
        class EitherMatcher(RequestMatcher):
            def match(self, request: Request) -> bool:
                if this._done:
                    return False
                if this._selected is not None:
                    matcher = this._matchers[this._selected]
                    return matcher.match(request)
                return this._get_endpoint_idx(request) is not None

            def __repr__(self) -> str:
                if this._done:
                    state = " DONE"
                elif this._selected is not None:
                    state = f" @{this._selected}"
                else:
                    state = ""
                return f"<{self.__class__.__name__} {this._matchers!r}{state}>"

        this: HttpEitherEndpoint = self

        return EitherMatcher(re.compile(".*"))

    def _handler(self) -> t.Callable[[Request], Response]:
        def handler(request: Request) -> Response:
            endpoint_idx: int | None
            if self._done:
                raise RuntimeError(
                    "Endpoint already done; "
                    "this should not happen, because matcher shouldn't "
                    "have matched the request. "
                    f"Request: {request!r}, matchers: {self._matchers!r}"
                )
            elif self._selected is not None:
                endpoint_idx = self._selected
            else:
                endpoint_idx = self._get_endpoint_idx(request)
                if endpoint_idx is None:
                    raise RuntimeError(
                        "No matching endpoint found for request, "
                        "this should not happen, because matcher shouldn't "
                        "have matched the request. "
                        f"Request: {request!r}, matchers: {self._matchers!r}"
                    )
                self._selected = endpoint_idx

            handler = self._handlers[endpoint_idx]
            endpoint = self._endpoints[endpoint_idx]
            res = handler(request)
            if isinstance(endpoint, HttpEndpointStateful):
                self._done = endpoint.done()
            else:
                self._done = True
            return res

        return handler

    def done(self) -> bool:
        return self._done
