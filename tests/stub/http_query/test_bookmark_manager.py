from __future__ import annotations

import typing as t
from contextlib import contextmanager

from nutkit import protocol as types
from nutkit.frontend import (
    BookmarkManager,
    Driver,
    Neo4jBookmarkManagerConfig,
)
from tests.shared import driver_feature
from tests.stub.http_query.shared import (
    http_types,
    HttpTestCase,
)
from tests.stub.http_query.shared.http_endpoint_builders import (
    TxEndpointBuilder,
)
from tests.stub.http_query.shared.http_endpoints import (
    HttpEndpoint,
    HttpQueryEndpoint,
    HttpSequenceEndpoint,
)
from tests.stub.http_query.shared.http_server import HandlerType

DB = "dba"
QUERY = "RETURN 1 AS n"
FIELDS = ["n"]
RECORDS: list[list[http_types.HttpType]] = [[http_types.Int(1)]]


def _make_query_endpoint(
    bookmarks_in: list[str], bookmarks_out: list[str]
) -> HttpEndpoint:
    return HttpQueryEndpoint(
        HttpQueryEndpoint.RequestData(
            db=DB,
            auth=HttpTestCase.AUTH,
            query=QUERY,
            bookmarks=bookmarks_in,
        ),
        HttpQueryEndpoint.ResponseData(
            fields=FIELDS,
            records=RECORDS,
            bookmarks=bookmarks_out,
        ),
    )


def _make_tx_endpoint(
    bookmarks_in: list[str], bookmarks_out: list[str]
) -> HttpEndpoint:
    return (
        TxEndpointBuilder(
            db=DB, auth=HttpTestCase.AUTH, bookmarks=bookmarks_in
        )
        .with_query(QUERY, FIELDS, RECORDS)
        .with_commit(bookmarks=bookmarks_out)
        .build()
    )


class TestBookmarkManager(HttpTestCase):
    required_features = (types.Feature.API_BOOKMARK_MANAGER,)

    def setUp(self) -> None:
        super().setUp()
        self._bookmark_managers: list[BookmarkManager] = []

    def tearDown(self) -> None:
        for bookmark_manager in self._bookmark_managers:
            bookmark_manager.close()
        self._bookmark_managers.clear()
        super().tearDown()

    def _new_bookmark_manager(
        self, initial_bookmarks: list[str] | None = None
    ) -> BookmarkManager:
        bookmark_manager = BookmarkManager(
            self._backend,
            Neo4jBookmarkManagerConfig(initial_bookmarks=initial_bookmarks),
        )
        self._bookmark_managers.append(bookmark_manager)
        return bookmark_manager

    @contextmanager
    def _driver_expecting(
        self, *endpoints: HttpEndpoint
    ) -> t.Generator[Driver]:
        with self.server() as server:
            server.install_discovery_endpoint()
            server.install_endpoint(
                HttpSequenceEndpoint(*endpoints),
                handler_type=HandlerType.PERMANENT,
            )
            with self.driver(server, self.AUTH) as driver:
                yield driver

    @staticmethod
    def _run_in_session(
        driver: Driver,
        bookmark_manager: BookmarkManager,
        bookmarks: list[str] | None = None,
    ) -> None:
        with driver.session(
            "w",
            database=DB,
            bookmarks=bookmarks,
            bookmark_manager=bookmark_manager,
        ) as session:
            list(session.run(QUERY))

    @staticmethod
    def _run_in_tx(
        driver: Driver,
        bookmark_manager: BookmarkManager,
        bookmarks: list[str] | None = None,
    ) -> None:
        with (
            driver.session(
                "w",
                database=DB,
                bookmarks=bookmarks,
                bookmark_manager=bookmark_manager,
            ) as session,
            session.begin_transaction() as tx,
        ):
            list(tx.run(QUERY))
            tx.commit()

    def test_should_keep_track_of_session_run(self) -> None:
        manager = self._new_bookmark_manager()
        with self._driver_expecting(
            _make_query_endpoint([], ["bm1"]),
            _make_query_endpoint(["bm1"], ["bm2"]),
            _make_query_endpoint(["bm2"], ["bm3"]),
        ) as driver:
            self._run_in_session(driver, manager)
            self._run_in_session(driver, manager)
            self._run_in_session(driver, manager)

    def test_should_keep_track_of_tx_in_sequence(self) -> None:
        manager = self._new_bookmark_manager()
        with self._driver_expecting(
            _make_tx_endpoint([], ["bm1"]),
            _make_tx_endpoint(["bm1"], ["bm2"]),
            _make_tx_endpoint(["bm2"], ["bm3"]),
        ) as driver:
            self._run_in_tx(driver, manager)
            self._run_in_tx(driver, manager)
            self._run_in_tx(driver, manager)

    def test_should_not_replace_bookmarks_with_empty_bookmarks(self) -> None:
        manager = self._new_bookmark_manager()
        with self._driver_expecting(
            _make_tx_endpoint([], ["bm1"]),
            _make_tx_endpoint(["bm1"], []),
            _make_tx_endpoint(["bm1"], ["bm3"]),
        ) as driver:
            self._run_in_tx(driver, manager)
            self._run_in_tx(driver, manager)
            self._run_in_tx(driver, manager)

    def test_should_send_all_bookmarks_and_replace_it_by_the_new_one(
        self,
    ) -> None:
        manager = self._new_bookmark_manager(
            initial_bookmarks=["first_bm", "adb:bm1"]
        )
        with self._driver_expecting(
            _make_tx_endpoint(["first_bm", "adb:bm1"], ["bm1"]),
            _make_tx_endpoint(["bm1"], ["bm2"]),
        ) as driver:
            self._run_in_tx(driver, manager)
            self._run_in_tx(driver, manager)

    def test_should_manage_explicitly_session_bookmarks(self) -> None:
        manager = self._new_bookmark_manager()
        with self._driver_expecting(
            _make_tx_endpoint([], ["bm1"]),
            _make_tx_endpoint(["bm1"], ["bm2"]),
            _make_tx_endpoint(["bm2", "unmanaged"], ["bm3"]),
            _make_tx_endpoint(["bm3"], ["bm4"]),
        ) as driver:
            self._run_in_tx(driver, manager)
            self._run_in_tx(driver, manager, bookmarks=[])
            self._run_in_tx(driver, manager, bookmarks=["bm2", "unmanaged"])
            self._run_in_tx(driver, manager)

    @driver_feature(types.Feature.API_DRIVER_EXECUTE_QUERY)
    def test_execute_query_should_chain_through_the_default_manager(
        self,
    ) -> None:
        with self._driver_expecting(
            _make_tx_endpoint([], ["bm1"]),
            _make_tx_endpoint(["bm1"], ["bm2"]),
        ) as driver:
            result1 = driver.execute_query(QUERY, database=DB)
            result2 = driver.execute_query(QUERY, database=DB)

        self.assertEqual(result1.keys, FIELDS)
        self.assertEqual(result2.keys, FIELDS)
