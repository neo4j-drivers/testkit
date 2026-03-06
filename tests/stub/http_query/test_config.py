from __future__ import annotations

import typing as t

from nutkit import protocol as types
from tests.stub.http_query.shared import (
    http_types,
    HttpTestCase,
)
from tests.stub.http_query.shared.http_endpoints import HttpQueryEndpoint
from tests.stub.http_query.shared.http_server import HandlerType

if t.TYPE_CHECKING:
    from werkzeug.datastructures import Headers

    T = t.TypeVar("T")
    TExc = t.TypeVar("TExc", bound=Exception)


DB = "dba"
QUERY = "RETURN 1 AS n"
FIELDS = ["n"]
RECORDS: list[list[http_types.HttpType]] = [[http_types.Int(1)]]
AUTH = HttpTestCase.AUTH
USER_AGENT = "Eesaiph0 einuWee8 vooHo4ku"


def _make_query_endpoint(
    expected_user_agent: str,
) -> HttpQueryEndpoint:
    def header_matcher(headers: Headers) -> bool:
        lower_user_agent_values = map(str.lower, headers.getlist("User-Agent"))
        return expected_user_agent.lower() not in lower_user_agent_values

    return HttpQueryEndpoint(
        HttpQueryEndpoint.RequestData(
            db=DB,
            auth=AUTH,
            query=QUERY,
        ),
        HttpQueryEndpoint.ResponseData(
            fields=FIELDS,
            records=RECORDS,
        ),
        extra_header_verification=(header_matcher,),
    )


class TestUserAgent(HttpTestCase):
    def test_session_run(self):
        # We expect the client not to send the configured user agent string,
        # because HTTP limits the characters that can be used in header values.
        #
        #     USER_AGENT = "BestDriverInTheWorld: '\"\t\x00\n\n\r\n\r\n🔥"
        #
        # for instance would totally break.

        # TODO: waiting for decision from PM, whether a technical solution
        #       needs to be found for transmitting arbitrary user agent strings

        query_endpoint = _make_query_endpoint(USER_AGENT)

        with self.server() as server:
            server.install_discovery_endpoint()
            server.install_endpoint(
                query_endpoint, handler_type=HandlerType.ONESHOT
            )
            with (
                self.driver(server, AUTH, user_agent=USER_AGENT) as driver,
                driver.session("w", database=DB) as session,
            ):
                result = session.run(QUERY)
                keys = result.keys()
                records = list(result)

        self.assertEqual(keys, FIELDS)
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0].values, [types.CypherInt(1)])
