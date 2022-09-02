from contextlib import contextmanager
import datetime

import pytz

import nutkit.protocol as types
from tests.neo4j.datatypes._base import (
    _TestTypesBase,
    MAX_INT64,
    MIN_INT64,
)
from tests.neo4j.datatypes._util import TZ_IDS
from tests.neo4j.shared import get_server_info
from tests.shared import get_driver_name


class TestDataTypes(_TestTypesBase):
    required_features = (types.Feature.API_TYPE_TEMPORAL,)

    # TODO: remove this as soon the 5.0 server implemented the UTC fix
    @contextmanager
    def expect_broken_utc_server(self, conditional=True):
        if (
            conditional
            and get_server_info().version >= "5.0"
            and self.driver_supports_features(types.Feature.BOLT_5_0)
        ):
            with self.assertRaises(Exception) as exc:
                yield
            if not isinstance(exc.exception,
                              (types.BaseError, AssertionError)):
                raise exc.exception
        else:
            yield

    def test_should_echo_temporal_type(self):
        vals = [
            types.CypherDate(1, 1, 1),
            types.CypherDate(1970, 1, 1),
            types.CypherDate(9999, 12, 31),

            types.CypherTime(0, 0, 0, 0),
            types.CypherTime(23, 59, 59, 999999999),
            types.CypherTime(23, 59, 59, 0, utc_offset_s=60 * 60 * 1),
            types.CypherTime(0, 0, 0, 0, utc_offset_s=60 * 60 * -1),
            # we are only testing utc offset down to 1 minute precision
            # while bolt supports up to 1 second precision, we don't expect
            # all driver languages to support that.
            types.CypherTime(23, 59, 59, 1, utc_offset_s=60),
            types.CypherTime(0, 0, 0, 0, utc_offset_s=-60),
            # current real world boundaries
            types.CypherTime(23, 59, 59, 0, utc_offset_s=60 * 60 * 14),
            types.CypherTime(0, 0, 0, 0, utc_offset_s=60 * 60 * -12),
            # server enforced boundaries
            types.CypherTime(23, 59, 59, 0, utc_offset_s=60 * 60 * 18),
            types.CypherTime(0, 0, 0, 0, utc_offset_s=60 * 60 * -18),

            types.CypherDateTime(1, 1, 1, 0, 0, 0, 0),
            types.CypherDateTime(1970, 1, 1, 0, 0, 0, 0),
            types.CypherDateTime(9999, 12, 31, 23, 59, 59, 999999999),
            types.CypherDateTime(1970, 1, 1, 0, 0, 0, 0,
                                 utc_offset_s=0),
            types.CypherDateTime(1970, 1, 1, 0, 0, 0, 0,
                                 utc_offset_s=60 * 60 * 18),
            types.CypherDateTime(1970, 1, 1, 0, 0, 0, 0,
                                 utc_offset_s=60 * 60 * -18),

            # # FIXME: this breaks because the bolt protocol needs fixing
            # # in the 80's the Swedes thought it was a good idea to introduce
            # # daylight saving time. LOL, fools!
            # # pre-shift 2:30 am UCT+2
            # types.CypherDateTime(1980, 9, 28, 2, 30, 0, 0,
            #                      utc_offset_s=60 * 60 * 2,
            #                      timezone_id="Europe/Stockholm"),
            # # one (non-political) hour later
            # # post-shift 2:30 am again but UCT+1
            # types.CypherDateTime(1980, 9, 28, 2, 30, 0, 0,
            #                      utc_offset_s=60 * 60 * 1,
            #                      timezone_id="Europe/Stockholm"),

            types.CypherDuration(0, 0, 0, 0),
            types.CypherDuration(0, 0, 0, -999999999),
            types.CypherDuration(1, 2, 3, 4),
            types.CypherDuration(-4, -3, -2, -1),
            types.CypherDuration(0, 0, MAX_INT64, 999999999),
            types.CypherDuration(0, MAX_INT64 // 86400, 0, 999999999),
            types.CypherDuration(MAX_INT64 // 2629746, 0, 0, 999999999),
            types.CypherDuration(0, 0, MIN_INT64, 0),
            # Note: `int(a / b) != a // b`
            #       `int(a / b)` rounds towards zero
            #       `a // b` rounds down (towards negative infinity)
            types.CypherDuration(0, int(MIN_INT64 / 86400), 0, 0),
            types.CypherDuration(int(MIN_INT64 / 2629746), 0, 0, 0),
        ]

        self._create_driver_and_session()
        for val in vals:
            with self.subTest(val=val):
                if (get_driver_name() in ["dotnet"]
                        and val.utc_offset_s is not None
                        and abs(val.utc_offset_s) > (14 * 60 * 60)):
                    self.skipTest("dotnet doesn't support larger"
                                  " than 14 hour offsets.")
                if get_driver_name() in ["python"]:
                    if (isinstance(val, types.CypherDateTime)
                            and val.utc_offset_s == 0):
                        self.skipTest(
                            "timezone library cannot tell the difference "
                            "between named UTC and 0s offset timezone"
                        )
                with self.expect_broken_utc_server(
                    isinstance(val, types.CypherDateTime)
                    and val.utc_offset_s is not None
                ):
                    self._verify_can_echo(val)

    def _timezone_server_support(self, tz_id):
        def work(tx):
            res = tx.run(
                f"RETURN datetime('1970-01-01T00:00:00.000[{tz_id}]').timezone"
            )
            rec = res.next()
            assert isinstance(rec, types.Record)
            assert isinstance(rec.values[0], types.CypherString)
            return rec.values[0].value

        assert self._driver and self._session
        try:
            echoed_tz_id = self._session.read_transaction(work)
            return echoed_tz_id == tz_id
        except types.DriverError as e:
            assert tz_id in e.msg
            return False

    def test_should_echo_all_timezone_ids(self):
        times = (
            # 1970-01-01 06:00:00
            (1970, 1, 1, 0, 0, 0, 0),
            # 2022-06-17 13:24:34.699546224
            (2022, 6, 17, 13, 24, 34, 699546224),
            # 2022-01-17 13:24:34.699546224
            (2022, 1, 17, 13, 24, 34, 699546224),
            # 0001-01-02 00:00:00
            (1, 1, 2, 0, 0, 0, 0),
        )

        self._create_driver_and_session()
        for tz_id in TZ_IDS:
            if not self._timezone_server_support(tz_id):
                continue
            for time in times:
                try:
                    tz = pytz.timezone(tz_id)
                except pytz.UnknownTimeZoneError:
                    # Can't test this timezone as Python doesn't know it :(
                    continue
                # FIXME: while there is a bug in the bolt protocol that
                #        makes it incapable of representing datetimes with
                #        timezone ids when there is ambiguity, we will
                #        avoid those.
                # ---------------------------------------------------------
                naive_dt = datetime.datetime(*time[:-1])
                dst_local_dt = tz.localize(naive_dt, is_dst=True)
                no_dst_local_dt = tz.localize(naive_dt, is_dst=False)
                while dst_local_dt != no_dst_local_dt:
                    naive_dt += datetime.timedelta(hours=1)
                    dst_local_dt = tz.localize(naive_dt, is_dst=True)
                    no_dst_local_dt = tz.localize(naive_dt, is_dst=False)
                # ---------------------------------------------------------

                dt = types.CypherDateTime(
                    naive_dt.year,
                    naive_dt.month,
                    naive_dt.day,
                    naive_dt.hour,
                    naive_dt.minute,
                    naive_dt.second,
                    time[-1],
                    utc_offset_s=dst_local_dt.utcoffset().total_seconds(),
                    timezone_id=tz_id
                )
                with self.subTest(dt=dt):
                    with self.expect_broken_utc_server():
                        self._verify_can_echo(dt)

    def test_date_time_cypher_created_tz_id(self):
        def work(tx):
            res = tx.run(
                f"WITH datetime('1970-01-01T10:08:09.000000001[{tz_id}]') "
                f"AS dt "
                f"RETURN dt, dt.year, dt.month, dt.day, dt.hour, dt.minute, "
                f"dt.second, dt.nanosecond, dt.offsetSeconds, dt.timezone"
            )
            rec = res.next()
            assert isinstance(rec, types.Record)
            dt_, y_, mo_, d_, h_, m_, s_, ns_, offset_, tz_ = rec.values
            assert isinstance(dt_, types.CypherDateTime)
            assert isinstance(y_, types.CypherInt)
            assert isinstance(mo_, types.CypherInt)
            assert isinstance(d_, types.CypherInt)
            assert isinstance(h_, types.CypherInt)
            assert isinstance(m_, types.CypherInt)
            assert isinstance(s_, types.CypherInt)
            assert isinstance(ns_, types.CypherInt)
            assert isinstance(offset_, types.CypherInt)
            assert isinstance(tz_, types.CypherString)

            return map(lambda x: getattr(x, "value", x), rec.values)

        self._create_driver_and_session()
        for tz_id in TZ_IDS:
            if not self._timezone_server_support(tz_id):
                continue
            with self.subTest(tz_id=tz_id):
                with self.expect_broken_utc_server():
                    dt, y, mo, d, h, m, s, ns, offset, tz = \
                        self._session.read_transaction(work)
                    self.assertEqual(dt.year, y)
                    self.assertEqual(dt.month, mo)
                    self.assertEqual(dt.day, d)
                    self.assertEqual(dt.hour, h)
                    self.assertEqual(dt.minute, m)
                    self.assertEqual(dt.second, s)
                    self.assertEqual(dt.nanosecond, ns)
                    # We are not testing the offset value because the server's
                    # and the driver's timezone db may diverge.
                    # self.assertEqual(dt.utc_offset_s, offset)
                    self.assertEqual(dt.timezone_id, tz)

    def test_date_components(self):
        self._create_driver_and_session()
        values = self._read_query_values(
            "CYPHER runtime=interpreted WITH $x AS x "
            "RETURN [x.year, x.month, x.day]",
            params={"x": types.CypherDate(2022, 3, 30)}
        )
        self.assertEqual(
            values,
            [types.CypherList(list(map(types.CypherInt, [2022, 3, 30])))]
        )

    def test_nested_date(self):
        data = types.CypherList(
            [types.CypherDate(2022, 3, 30), types.CypherDate(1976, 6, 13)]
        )
        self._create_driver_and_session()
        values = self._write_query_values(
            "CREATE (a {x:$x}) RETURN a.x",
            params={"x": data}
        )
        self.assertEqual(values, [data])

    def test_cypher_created_date(self):
        self._create_driver_and_session()
        values = self._read_query_values("RETURN date('1976-06-13')")
        self.assertEqual(
            values,
            [types.CypherDate(1976, 6, 13)]
        )

    def test_time_components(self):
        self._create_driver_and_session()
        values = self._read_query_values(
            "CYPHER runtime=interpreted WITH $x AS x "
            "RETURN [x.hour, x.minute, x.second, x.nanosecond]",
            params={"x": types.CypherTime(13, 24, 34, 699546224)}
        )
        self.assertEqual(
            values,
            [types.CypherList(list(map(types.CypherInt,
                                       [13, 24, 34, 699546224])))]
        )

    def test_time_with_offset_components(self):
        self._create_driver_and_session()
        values = self._read_query_values(
            "CYPHER runtime=interpreted WITH $x AS x "
            "RETURN [x.hour, x.minute, x.second, x.nanosecond, x.offset]",
            params={"x": types.CypherTime(13, 24, 34, 699546224,
                                          utc_offset_s=-5520)}
        )
        self.assertEqual(
            values,
            [
                types.CypherList([
                    types.CypherInt(13),
                    types.CypherInt(24),
                    types.CypherInt(34),
                    types.CypherInt(699546224),
                    types.CypherString("-01:32")
                ])
            ]
        )

    def test_nested_time(self):
        data = types.CypherList([
            types.CypherTime(13, 24, 34, 699546224),
            types.CypherTime(23, 25, 34, 699547625)
        ])
        t = types.CypherTime(23, 25, 34, 699547625, utc_offset_s=-5520)
        self._create_driver_and_session()
        values = self._write_query_values(
            "CREATE (a {x:$x, y:$y}) RETURN a.x, a.y",
            params={"x": data, "y": t}
        )
        self.assertEqual(values, [data, t])

    def test_cypher_created_time(self):
        for (s, t) in (
            (
                "time('13:24:34')",
                types.CypherTime(13, 24, 34, 0, utc_offset_s=0)
            ),
            (
                "time('13:24:34.699546224')",
                types.CypherTime(13, 24, 34, 699546224, utc_offset_s=0)
            ),
            (
                "time('12:34:56.789012345+0130')",
                types.CypherTime(12, 34, 56, 789012345, utc_offset_s=5400)
            ),
            (
                "time('12:34:56.789012345-01:30')",
                types.CypherTime(12, 34, 56, 789012345, utc_offset_s=-5400)
            ),
            (
                "time('12:34:56.789012345Z')",
                types.CypherTime(12, 34, 56, 789012345, utc_offset_s=0)
            ),
            (
                "localtime('12:34:56.789012345')",
                types.CypherTime(12, 34, 56, 789012345)
            ),
        ):
            with self.subTest(s=s, t=t):
                self._create_driver_and_session()
                values = self._read_query_values(f"RETURN {s}")
                self.assertEqual(values, [t])

    def test_datetime_components(self):
        self._create_driver_and_session()
        values = self._read_query_values(
            "CYPHER runtime=interpreted WITH $x AS x "
            "RETURN [x.year, x.month, x.day, x.hour, x.minute, x.second, "
            "x.nanosecond]",
            params={"x": types.CypherDateTime(2022, 3, 30, 13, 24, 34,
                                              699546224)}
        )
        self.assertEqual(
            values,
            [
                types.CypherList(list(map(
                    types.CypherInt,
                    [2022, 3, 30, 13, 24, 34, 699546224]
                )))
            ]
        )

    def test_datetime_with_offset_components(self):
        self._create_driver_and_session()
        with self.expect_broken_utc_server():
            values = self._read_query_values(
                "CYPHER runtime=interpreted WITH $x AS x "
                "RETURN [x.year, x.month, x.day, x.hour, x.minute, x.second, "
                "x.nanosecond, x.offset]",
                params={
                    "x": types.CypherDateTime(
                        2022, 3, 30, 13, 24, 34, 699546224, utc_offset_s=-5520
                    ),
                }
            )
            self.assertEqual(
                values,
                [
                    types.CypherList([
                        types.CypherInt(2022),
                        types.CypherInt(3),
                        types.CypherInt(30),
                        types.CypherInt(13),
                        types.CypherInt(24),
                        types.CypherInt(34),
                        types.CypherInt(699546224),
                        types.CypherString("-01:32")
                    ])
                ]
            )

    def test_datetime_with_timezone_components(self):
        self._create_driver_and_session()
        with self.expect_broken_utc_server():
            values = self._read_query_values(
                "CYPHER runtime=interpreted WITH $x AS x "
                "RETURN [x.year, x.month, x.day, x.hour, x.minute, x.second, "
                "x.nanosecond, x.offset, x.timezone]",
                params={"x": types.CypherDateTime(
                    2022, 3, 30, 13, 24, 34, 699546224,
                    utc_offset_s=-14400, timezone_id="America/Toronto"
                )}
            )
            self.assertEqual(
                values,
                [
                    types.CypherList([
                        types.CypherInt(2022),
                        types.CypherInt(3),
                        types.CypherInt(30),
                        types.CypherInt(13),
                        types.CypherInt(24),
                        types.CypherInt(34),
                        types.CypherInt(699546224),
                        types.CypherString("-04:00"),
                        types.CypherString("America/Toronto")
                    ])
                ]
            )

    def test_nested_datetime(self):
        data = types.CypherList([
            types.CypherDateTime(2018, 4, 6, 13, 4, 42, 516120123),
            types.CypherDateTime(2022, 3, 30, 0, 0, 0, 0)
        ])
        dt1 = types.CypherDateTime(2022, 3, 30, 0, 0, 0, 0, utc_offset_s=-5520)
        dt2 = types.CypherDateTime(
            2022, 3, 30, 13, 24, 34, 699546224,
            utc_offset_s=-14400, timezone_id="America/Toronto"
        )
        self._create_driver_and_session()
        with self.expect_broken_utc_server():
            values = self._write_query_values(
                "CREATE (a {x:$x, y:$y, z:$z}) RETURN a.x, a.y, a.z",
                params={"x": data, "y": dt1, "z": dt2}
            )
            self.assertEqual(values, [data, dt1, dt2])

    def test_cypher_created_datetime(self):
        for (s, dt) in (
            (
                "datetime('1976-06-13T12:34:56')",
                types.CypherDateTime(1976, 6, 13, 12, 34, 56, 0,
                                     utc_offset_s=0)
            ),
            (
                "datetime('1976-06-13T12:34:56[UTC]')",
                types.CypherDateTime(1976, 6, 13, 12, 34, 56, 0,
                                     utc_offset_s=0, timezone_id="UTC")
            ),
            (
                "datetime('1976-06-13T12:34:56.999888777')",
                types.CypherDateTime(1976, 6, 13, 12, 34, 56, 999888777,
                                     utc_offset_s=0)
            ),
            (
                "datetime('1976-06-13T12:34:56.999888777-05:00')",
                types.CypherDateTime(1976, 6, 13, 12, 34, 56, 999888777,
                                     utc_offset_s=-18000)
            ),
            (
                "datetime('1976-06-13T12:34:56.789012345+01:00"
                "[Europe/London]')",
                types.CypherDateTime(
                    1976, 6, 13, 12, 34, 56, 789012345,
                    utc_offset_s=3600, timezone_id="Europe/London"
                )
            ),
            (
                "localdatetime('1976-06-13T12:34:56')",
                types.CypherDateTime(1976, 6, 13, 12, 34, 56, 0)
            ),
            (
                "localdatetime('1976-06-13T12:34:56.123')",
                types.CypherDateTime(1976, 6, 13, 12, 34, 56, 123000000)
            ),
        ):
            with self.subTest(s=s, dt=dt):
                self._create_driver_and_session()
                with self.expect_broken_utc_server(
                    dt.utc_offset_s is not None
                ):
                    values = self._read_query_values(f"RETURN {s}")
                    if get_driver_name() == "python":
                        # Python cannot discriminate between +00:00
                        # and +00:00[UTC]
                        assert isinstance(dt, types.CypherDateTime)
                        if dt.utc_offset_s == 0 and dt.timezone_id is None:
                            dt.timezone_id = "UTC"
                    self.assertEqual(values, [dt])

    def test_duration_components(self):
        for (mo, d, s, ns_os, ns) in (
            (3, 4, 999, 123456789, 999_123456789),
            (0, 0, MAX_INT64, 999999999, -1),  # LUL, Cypher overflows
        ):
            with self.subTest(mo=mo, d=d, s=s, ns=ns):
                self._create_driver_and_session()
                values = self._read_query_values(
                    "CYPHER runtime=interpreted WITH $x AS x "
                    "RETURN [x.months, x.days, x.seconds, "
                    "x.nanosecondsOfSecond, x.nanoseconds]",
                    params={"x": types.CypherDuration(
                        months=mo, days=d, seconds=s, nanoseconds=ns_os
                    )}
                )
                self.assertEqual(
                    values,
                    [types.CypherList(list(map(types.CypherInt,
                                               [mo, d, s, ns_os, ns])))]
                )

    def test_nested_duration(self):
        data = types.CypherList([
            types.CypherDuration(months=3, days=4, seconds=999,
                                 nanoseconds=123456789),
            types.CypherDuration(months=0, days=0, seconds=MAX_INT64,
                                 nanoseconds=999999999)
        ])
        self._create_driver_and_session()
        values = self._write_query_values("CREATE (a {x:$x}) RETURN a.x",
                                          params={"x": data})
        self.assertEqual(values, [data])

    def test_cypher_created_duration(self):
        for (s, d) in (
            (
                "duration('P1234M567890DT123456789123.999S')",
                types.CypherDuration(1234, 567890, 123456789123, 999000000)
            ),
            (
                "duration('P1Y2M3W4DT5H6M7.999888777S')",
                types.CypherDuration(
                    12 * 1 + 2,
                    7 * 3 + 4,
                    5 * 60 * 60 + 6 * 60 + 7,
                    999888777
                )
            ),
        ):
            with self.subTest(s=s, d=d):
                self._create_driver_and_session()
                values = self._read_query_values(f"RETURN {s}")
                self.assertEqual(values, [d])
