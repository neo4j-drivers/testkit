from __future__ import annotations

import typing as t

from nutkit import protocol as types
from tests.shared import Potential

from .. import http_types
from ..http_endpoints import (
    AnyValue,
    AutoRespond,
    CountersMap,
    HttpEitherEndpoint,
    HttpEndpoint,
    HttpSequenceEndpoint,
    HttpTxCommitEndpoint,
    HttpTxEndpoint,
    HttpTxQueryEndpoint,
    HttpTxRollbackEndpoint,
    MaybeNull,
    Notification,
    Plan,
    Profile,
)

if t.TYPE_CHECKING:
    import re

    from werkzeug.datastructures import Headers

    from ..http_endpoints import TOptionalValue


_ANY_VALUE = AnyValue()
_DEFAULT_PARAMETERS: MaybeNull[dict[str, http_types.HttpType]] = MaybeNull({})
_AUTO_RESPOND = AutoRespond()


class TxEndpointBuilder:
    _db: str | re.Pattern[str]
    _auth: types.AuthorizationToken
    _pipeline_begin: Potential
    _tx_id: str
    _pipelined_handlers: list[HttpEndpoint]
    _sequential_handlers: list[HttpEndpoint]
    _finishing_handler: HttpEndpoint | None
    _used: bool = False
    _impersonated_user: str | None
    _access_mode: TOptionalValue[str] | AnyValue
    _bookmarks: list[str]
    _tx_errors: list[dict[str, object]] | None
    _affinity_header: str | None
    _extra_body_verification: tuple[t.Callable[[dict[str, object]], bool], ...]
    _extra_header_verification: tuple[t.Callable[[Headers], bool], ...]

    def __init__(
        self,
        db: str | re.Pattern[str],
        auth: types.AuthorizationToken,
        pipeline_begin: Potential = Potential.MAYBE,
        tx_id: str = "txid123",
        impersonated_user: str | None = None,
        access_mode: TOptionalValue[str] | AnyValue = _ANY_VALUE,
        bookmarks: list[str] | None = None,
        tx_errors: list[dict[str, object]] | None = None,
        affinity_header: str | None = None,
        extra_body_verification: tuple[
            t.Callable[[dict[str, object]], bool], ...
        ] = (),
        extra_header_verification: tuple[
            t.Callable[[Headers], bool], ...
        ] = (),
    ) -> None:
        if bookmarks is None:
            bookmarks = []
        self._db = db
        self._auth = auth
        self._pipeline_begin = pipeline_begin
        self._tx_id = tx_id
        self._finishing_handler = None
        self._impersonated_user = impersonated_user
        self._access_mode = access_mode
        self._bookmarks = bookmarks
        self._affinity_header = affinity_header
        self._tx_errors = tx_errors
        self._extra_body_verification = extra_body_verification
        self._extra_header_verification = extra_header_verification
        self._pipelined_handlers = []
        self._sequential_handlers = [
            self._make_tx_handler(
                impersonated_user=impersonated_user,
                access_mode=access_mode,
                bookmarks=bookmarks,
                errors=tx_errors,
            ),
        ]

    def with_query(
        self,
        query: str,
        fields: list[str] | None,
        records: list[list[http_types.HttpType]] | None,
        parameters: (  # noqa: PAR001
            TOptionalValue[dict[str, http_types.HttpType]] | None
        ) = _DEFAULT_PARAMETERS,
        include_counters: TOptionalValue[bool] | AnyValue | None = _ANY_VALUE,
        counters: CountersMap | AutoRespond | None = _AUTO_RESPOND,
        plan: Plan | None = None,
        profile: Profile | None = None,
        notifications: list[Notification] | None = None,
        query_errors: list[dict[str, object]] | None = None,
        extra_body_verification: tuple[
            t.Callable[[dict[str, object]], bool], ...
        ] = (),
        extra_header_verification: tuple[
            t.Callable[[Headers], bool], ...
        ] = (),
    ) -> t.Self:
        if self._tx_errors is not None and query_errors is not None:
            raise ValueError(
                "Cannot set query errors when transaction errors are already "
                "set"
            )
        if self._pipeline_begin in {Potential.YES, Potential.MAYBE}:
            if not self._pipelined_handlers:
                self._pipelined_handlers.append(
                    self._make_tx_handler(
                        query,
                        fields,
                        records,
                        self._impersonated_user,
                        self._access_mode,
                        self._bookmarks,
                        parameters,
                        include_counters,
                        counters,
                        plan,
                        profile,
                        notifications,
                        errors=self._tx_errors,
                        extra_body_verification=(
                            *self._extra_body_verification,
                            *extra_body_verification,
                        ),
                        extra_header_verification=(
                            *self._extra_header_verification,
                            *extra_header_verification,
                        ),
                    )
                )
            else:
                self._pipelined_handlers.append(
                    self._make_query_handler(
                        query,
                        fields,
                        records,
                        parameters,
                        include_counters,
                        counters,
                        plan,
                        profile,
                        notifications,
                        errors=query_errors,
                        extra_body_verification=(
                            *self._extra_body_verification,
                            *extra_body_verification,
                        ),
                        extra_header_verification=(
                            self._verify_affinity_header,
                            *self._extra_header_verification,
                            *extra_header_verification,
                        ),
                    )
                )
        if self._pipeline_begin in {Potential.NO, Potential.MAYBE}:
            self._sequential_handlers.append(
                self._make_query_handler(
                    query,
                    fields,
                    records,
                    parameters,
                    include_counters,
                    counters,
                    plan,
                    profile,
                    notifications,
                    errors=query_errors,
                    extra_body_verification=(
                        *self._extra_body_verification,
                        *extra_body_verification,
                    ),
                    extra_header_verification=(
                        self._verify_affinity_header,
                        *self._extra_header_verification,
                        *extra_header_verification,
                    ),
                )
            )
        return self

    def with_commit(
        self,
        bookmarks: list[str] | None = None,
    ) -> t.Self:
        self._finishing_handler = HttpTxCommitEndpoint(
            HttpTxCommitEndpoint.RequestData(
                db=self._db, tx_id=self._tx_id, auth=self._auth
            ),
            HttpTxCommitEndpoint.ResponseData(bookmarks=bookmarks),
        )
        return self

    def with_rollback(self) -> t.Self:
        self._finishing_handler = HttpTxRollbackEndpoint(
            HttpTxRollbackEndpoint.RequestData(
                db=self._db, tx_id=self._tx_id, auth=self._auth
            ),
        )
        return self

    def without_finishing_handler(self) -> t.Self:
        self._finishing_handler = None
        return self

    def build(self) -> HttpEndpoint:
        assert self._sequential_handlers, "initialized non-empty int __init__"
        if self._used:
            raise RuntimeError(
                "This builder has already been used to build an endpoint"
            )
        self._used = True
        endpoints: list[HttpEndpoint] = []
        if self._pipelined_handlers:
            endpoints.append(
                HttpEitherEndpoint(
                    self._make_sequential(self._pipelined_handlers),
                    self._make_sequential(self._sequential_handlers),
                )
            )
        else:
            endpoints.extend(self._sequential_handlers)
        if self._finishing_handler is not None:
            endpoints.append(self._finishing_handler)
        return self._make_sequential(endpoints)

    def _make_tx_handler(
        self,
        query: str | None = None,
        fields: list[str] | None = None,
        records: list[list[http_types.HttpType]] | None = None,
        impersonated_user: str | None = None,
        access_mode: TOptionalValue[str] | AnyValue = _ANY_VALUE,
        bookmarks: list[str] | None = None,
        parameters: (  # noqa: PAR001
            TOptionalValue[dict[str, http_types.HttpType]] | None
        ) = None,
        include_counters: TOptionalValue[bool] | AnyValue | None = None,
        counters: CountersMap | AutoRespond | None = None,
        plan: Plan | None = None,
        profile: Profile | None = None,
        notifications: list[Notification] | None = None,
        errors: list[dict[str, object]] | None = None,
        extra_body_verification: tuple[
            t.Callable[[dict[str, object]], bool], ...
        ] = (),
        extra_header_verification: tuple[
            t.Callable[[Headers], bool], ...
        ] = (),
    ) -> HttpEndpoint:
        if bookmarks is None:
            bookmarks = []
        tx_data = None
        if not errors:
            tx_data = HttpTxEndpoint.ResponseData.Tx(id=self._tx_id)
        if query is not None:
            return HttpTxEndpoint(
                HttpTxEndpoint.RequestData(
                    db=self._db,
                    auth=self._auth,
                    query=query,
                    impersonated_user=impersonated_user,
                    access_mode=access_mode,
                    parameters=parameters,
                    bookmarks=bookmarks,
                    include_counters=include_counters,
                ),
                HttpTxEndpoint.ResponseData(
                    tx_data,
                    fields=fields,
                    records=records,
                    affinity_header=self._affinity_header,
                    counters=counters,
                    plan=plan,
                    profile=profile,
                    notifications=notifications,
                    errors=errors,
                ),
                extra_body_verification=extra_body_verification,
                extra_header_verification=extra_header_verification,
            )
        else:
            return HttpTxEndpoint(
                HttpTxEndpoint.RequestData(
                    db=self._db,
                    auth=self._auth,
                    impersonated_user=impersonated_user,
                    access_mode=access_mode,
                    bookmarks=bookmarks,
                ),
                HttpTxEndpoint.ResponseData(
                    tx_data,
                    affinity_header=self._affinity_header,
                    errors=errors,
                ),
                extra_body_verification=extra_body_verification,
                extra_header_verification=extra_header_verification,
            )

    def _make_query_handler(
        self,
        query: str,
        fields: list[str] | None,
        records: list[list[http_types.HttpType]] | None,
        parameters: TOptionalValue[dict[str, http_types.HttpType]] | None,
        include_counters: TOptionalValue[bool] | AnyValue | None,
        counters: CountersMap | AutoRespond | None,
        plan: Plan | None = None,
        profile: Profile | None = None,
        notifications: list[Notification] | None = None,
        errors: list[dict[str, object]] | None = None,
        extra_body_verification: tuple[
            t.Callable[[dict[str, object]], bool], ...
        ] = (),
        extra_header_verification: tuple[
            t.Callable[[Headers], bool], ...
        ] = (),
    ) -> HttpEndpoint:
        tx_data = None
        if not errors:
            tx_data = HttpTxQueryEndpoint.ResponseData.Tx(id=self._tx_id)
        return HttpTxQueryEndpoint(
            HttpTxQueryEndpoint.RequestData(
                db=self._db,
                tx_id=self._tx_id,
                auth=self._auth,
                query=query,
                parameters=parameters,
                include_counters=include_counters,
            ),
            HttpTxQueryEndpoint.ResponseData(
                tx_data,
                fields=fields,
                records=records,
                counters=counters,
                plan=plan,
                profile=profile,
                notifications=notifications,
                errors=errors,
            ),
            extra_body_verification=extra_body_verification,
            extra_header_verification=extra_header_verification,
        )

    @classmethod
    def _make_sequential(cls, endpoints: list[HttpEndpoint]) -> HttpEndpoint:
        assert endpoints, "must not be empty"
        if len(endpoints) == 1:
            return endpoints[0]
        else:
            return HttpSequenceEndpoint(*endpoints)

    def _verify_affinity_header(self, headers: Headers) -> bool:
        affinity_values = headers.getlist("neo4j-cluster-affinity")
        if self._affinity_header is None:
            return not affinity_values
        return [self._affinity_header] == affinity_values
