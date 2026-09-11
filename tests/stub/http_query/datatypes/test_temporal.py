from __future__ import annotations

import copy
import dataclasses

from nutkit import protocol as types
from tests.shared import (
    AVERAGE_SECONDS_IN_DAY,
    AVERAGE_SECONDS_IN_MONTH,
    get_driver_name,
    MAX_INT64,
    MIN_INT64,
)
from tests.stub.http_query.shared import (
    http_types,
    HttpTestCase,
)


class TestTemporal(HttpTestCase):
    def test_date(self):
        with self.server() as server:
            for value in (
                types.CypherDate(1970, 1, 1),
                types.CypherDate(2024, 2, 29),
                types.CypherDate(1900, 2, 28),
                types.CypherDate(1, 1, 1),
                types.CypherDate(9999, 12, 31),
                types.CypherDate(0, 1, 1),
                types.CypherDate(-1, 1, 1),
                types.CypherDate(-200, 1, 1),
                types.CypherDate(-10000, 1, 1),
                types.CypherDate(-10000, 1, 1),
                types.CypherDate(10000, 1, 1),
                types.CypherDate(999999, 12, 31),
                types.CypherDate(40000, 2, 29),
                types.CypherDate(-40000, 2, 29),
                types.CypherDate(0, 2, 29),
            ):
                with self.subTest(x=value):
                    self._echo_session_run(
                        server,
                        value,
                        http_types.HttpType.from_cypher_type(value),
                    )

    def test_zoned_time(self):
        with self.server() as server:
            cases = [
                (
                    types.CypherTime(13, 45, 30, 123456789, 0),
                    http_types.ZonedTime(13, 45, 30, 123456789, 0),
                ),
                (
                    types.CypherTime(0, 0, 0, 0, -86400),
                    http_types.ZonedTime(0, 0, 0, 0, -86400),
                ),
                (
                    types.CypherTime(0, 0, 0, 0, -64800),
                    http_types.ZonedTime(0, 0, 0, 0, -64800),
                ),
                (
                    types.CypherTime(23, 59, 59, 999999999, 86400),
                    http_types.ZonedTime(23, 59, 59, 999999999, 86400),
                ),
                (
                    types.CypherTime(23, 59, 59, 999999999, 64800),
                    http_types.ZonedTime(23, 59, 59, 999999999, 64800),
                ),
                (
                    types.CypherTime(13, 45, 30, 123456789, 3661),
                    http_types.ZonedTime(13, 45, 30, 123456789, 3661),
                ),
                (
                    types.CypherTime(13, 45, 30, 123456789, -3661),
                    http_types.ZonedTime(13, 45, 30, 123456789, -3661),
                ),
                (
                    types.CypherTime(0, 0, 0, 0, -60),
                    http_types.ZonedTime(0, 0, 0, 0, -60),
                ),
                (
                    types.CypherTime(0, 0, 0, 0, 60),
                    http_types.ZonedTime(0, 0, 0, 0, 60),
                ),
                (
                    types.CypherTime(0, 0, 0, 0, -64920),
                    http_types.ZonedTime(0, 0, 0, 0, -64920),
                ),
                (
                    types.CypherTime(0, 0, 0, 0, 64920),
                    http_types.ZonedTime(0, 0, 0, 0, 64920),
                ),
            ]

            for cypher_value, http_value in cases:
                if not self.should_run_subtest(x=cypher_value):
                    continue
                with self.uncheckedSubTest(x=cypher_value):
                    self._echo_session_run(
                        server,
                        cypher_value,
                        http_value,
                    )

    def test_zoned_time_formats(self):
        with self.server() as server:
            for fmt in (
                http_types.ZonedTime.SerializationFormat(
                    use_zulu=False,
                ),
                http_types.ZonedTime.SerializationFormat(
                    omit_zero_seconds=True,
                ),
                http_types.ZonedTime.SerializationFormat(
                    omit_zero_nanoseconds=False,
                ),
                http_types.ZonedTime.SerializationFormat(
                    omit_zero_nanoseconds=False,
                    trim_nanoseconds=False,
                ),
            ):
                with self.subTest(format=dataclasses.asdict(fmt)):
                    cypher_value = types.CypherTime(0, 0, 0, 0, 0)
                    http_value = http_types.ZonedTime(0, 0, 0, 0, 0, fmt)
                    self._echo_session_run(
                        server,
                        cypher_value,
                        http_value,
                    )

    def test_local_time(self):
        with self.server() as server:
            for cypher_value, http_value in (
                (
                    types.CypherTime(13, 45, 30, 123456789),
                    http_types.LocalTime(13, 45, 30, 123456789),
                ),
                (
                    types.CypherTime(0, 0, 0, 0),
                    http_types.LocalTime(0, 0, 0, 0),
                ),
                (
                    types.CypherTime(23, 59, 59, 999999999),
                    http_types.LocalTime(23, 59, 59, 999999999),
                ),
            ):
                with self.subTest(x=cypher_value):
                    self._echo_session_run(
                        server,
                        cypher_value,
                        http_value,
                    )

    def test_local_time_formats(self):
        with self.server() as server:
            for fmt in (
                http_types.LocalTime.SerializationFormat(
                    omit_zero_seconds=True,
                ),
                http_types.LocalTime.SerializationFormat(
                    omit_zero_nanoseconds=False,
                ),
                http_types.LocalTime.SerializationFormat(
                    omit_zero_nanoseconds=False,
                    trim_nanoseconds=False,
                ),
            ):
                with self.subTest(format=dataclasses.asdict(fmt)):
                    cypher_value = types.CypherTime(0, 0, 0, 0)
                    http_value = http_types.LocalTime(0, 0, 0, 0, fmt)
                    self._echo_session_run(
                        server,
                        cypher_value,
                        http_value,
                    )

    def test_zoned_date_time(self):
        dt1 = (2025, 1, 1, 13, 45, 30, 123456789)
        dt1_minus_2_h = (2025, 1, 1, 11, 45, 30, 123456789)

        with self.server() as server:
            for cypher_value, http_value in (
                (
                    types.CypherDateTime(*dt1, 3600, "Europe/Berlin"),
                    http_types.ZonedDateTime(*dt1, 3600, "Europe/Berlin"),
                ),
                (
                    types.CypherDateTime(*dt1, 50400, "Pacific/Kiritimati"),
                    http_types.ZonedDateTime(
                        *dt1, 50400, "Pacific/Kiritimati"
                    ),
                ),
                (
                    types.CypherDateTime(*dt1, -39600, "Pacific/Niue"),
                    http_types.ZonedDateTime(*dt1, -39600, "Pacific/Niue"),
                ),
                (
                    types.CypherDateTime(*dt1, 0, "Africa/Abidjan"),
                    http_types.ZonedDateTime(*dt1, 0, "Africa/Abidjan"),
                ),
                # Server disagrees with offset, driver should cope
                (
                    types.CypherDateTime(*dt1, 3600, "Europe/Berlin"),
                    http_types.ZonedDateTime(
                        *dt1_minus_2_h, -3600, "Europe/Berlin"
                    ),
                ),
            ):
                with self.subTest(x=cypher_value):
                    self._echo_session_run(
                        server,
                        cypher_value,
                        http_value,
                    )

    def test_zoned_date_time_formats(self):
        with self.server() as server:
            for fmt in (
                http_types.ZonedDateTime.SerializationFormat(
                    use_zulu=False,
                ),
                http_types.ZonedDateTime.SerializationFormat(
                    omit_zero_seconds=True,
                ),
                http_types.ZonedDateTime.SerializationFormat(
                    omit_zero_nanoseconds=False,
                ),
                http_types.ZonedDateTime.SerializationFormat(
                    omit_zero_nanoseconds=False,
                    trim_nanoseconds=False,
                ),
            ):
                with self.subTest(format=dataclasses.asdict(fmt)):
                    tz_name = "Etc/GMT"
                    dt_args = (800, 1, 1, 0, 0, 0, 0, 0, tz_name)
                    cypher_value = types.CypherDateTime(*dt_args)
                    http_value = http_types.ZonedDateTime(*dt_args, fmt=fmt)
                    self._echo_session_run(
                        server,
                        cypher_value,
                        http_value,
                    )

    def test_offset_date_time(self):
        dt1 = (2025, 1, 1, 13, 45, 30, 123456789)

        with self.server() as server:
            for cypher_value, http_value in (
                (
                    types.CypherDateTime(*dt1, 3600),
                    http_types.OffsetDateTime(*dt1, 3600),
                ),
                (
                    types.CypherDateTime(*dt1, 50400),
                    http_types.OffsetDateTime(*dt1, 50400),
                ),
                (
                    types.CypherDateTime(*dt1, -39600),
                    http_types.OffsetDateTime(*dt1, -39600),
                ),
                (
                    types.CypherDateTime(*dt1, 0),
                    http_types.OffsetDateTime(*dt1, 0),
                ),
            ):
                with self.subTest(x=cypher_value):
                    cypher_value_out = cypher_value
                    http_value_out = http_value
                    if (
                        http_value.utc_offset_s == 0
                        and get_driver_name() == "python"
                    ):
                        # Python cannot discriminate between +00:00
                        # and +00:00[UTC]
                        http_value = http_types.ZonedDateTime(
                            http_value.year,
                            http_value.month,
                            http_value.day,
                            http_value.hour,
                            http_value.minute,
                            http_value.second,
                            http_value.nanosecond,
                            http_value.utc_offset_s,
                            "UTC",
                        )
                        cypher_value_out = copy.copy(cypher_value)
                        cypher_value_out.timezone_id = "UTC"
                    self._echo_session_run(
                        server,
                        cypher_value,
                        http_value,
                        http_value_out=http_value_out,
                        cypher_value_out=cypher_value_out,
                    )

    def test_offset_date_time_formats(self):
        with self.server() as server:
            for fmt in (
                http_types.OffsetDateTime.SerializationFormat(
                    use_zulu=False,
                ),
                http_types.OffsetDateTime.SerializationFormat(
                    omit_zero_seconds=True,
                ),
                http_types.OffsetDateTime.SerializationFormat(
                    omit_zero_nanoseconds=False,
                ),
                http_types.OffsetDateTime.SerializationFormat(
                    omit_zero_nanoseconds=False,
                    trim_nanoseconds=False,
                ),
            ):
                with self.subTest(format=dataclasses.asdict(fmt)):
                    offset = 0 if not fmt.use_zulu else -4500
                    dt_args = (800, 1, 1, 0, 0, 0, 0, offset)
                    cypher_value = types.CypherDateTime(*dt_args)
                    cypher_value_out = cypher_value
                    http_value = http_types.OffsetDateTime(*dt_args, fmt=fmt)
                    http_value_out = http_value
                    if offset == 0 and get_driver_name() == "python":
                        # Python cannot discriminate between +00:00
                        # and +00:00[UTC]
                        http_value = http_types.ZonedDateTime(
                            *dt_args, "UTC", fmt=fmt
                        )
                        cypher_value_out = types.CypherDateTime(
                            *dt_args, "UTC"
                        )
                    self._echo_session_run(
                        server,
                        cypher_value,
                        http_value,
                        http_value_out=http_value_out,
                        cypher_value_out=cypher_value_out,
                    )

    def test_local_date_time(self):
        ns_max = 999_999_999
        with self.server() as server:
            for cypher_value, http_value in (
                (
                    types.CypherDateTime(1, 1, 1, 0, 0, 0, 0),
                    http_types.LocalDateTime(1, 1, 1, 0, 0, 0, 0),
                ),
                (
                    types.CypherDateTime(44, 2, 29, 23, 59, 59, ns_max),
                    http_types.LocalDateTime(44, 2, 29, 23, 59, 59, ns_max),
                ),
                (
                    types.CypherDateTime(2026, 1, 27, 9, 40, 48, 1000),
                    http_types.LocalDateTime(2026, 1, 27, 9, 40, 48, 1000),
                ),
                (
                    types.CypherDateTime(9999, 12, 31, 23, 59, 59, ns_max),
                    http_types.LocalDateTime(9999, 12, 31, 23, 59, 59, ns_max),
                ),
                (
                    types.CypherDateTime(25618, 12, 31, 23, 59, 59, 0),
                    http_types.LocalDateTime(25618, 12, 31, 23, 59, 59, 0),
                ),
                (
                    types.CypherDateTime(0, 12, 24, 22, 21, 23, 0),
                    http_types.LocalDateTime(0, 12, 24, 22, 21, 23, 0),
                ),
                (
                    types.CypherDateTime(-1, 7, 11, 13, 17, 19, 23),
                    http_types.LocalDateTime(-1, 7, 11, 13, 17, 19, 23),
                ),
                (
                    types.CypherDateTime(-25618, 12, 31, 23, 59, 59, 0),
                    http_types.LocalDateTime(-25618, 12, 31, 23, 59, 59, 0),
                ),
            ):
                with self.subTest(x=cypher_value):
                    self._echo_session_run(
                        server,
                        cypher_value,
                        http_value,
                    )

    def test_local_date_time_formats(self):
        with self.server() as server:
            for fmt in (
                http_types.LocalDateTime.SerializationFormat(
                    omit_zero_seconds=True,
                ),
                http_types.LocalDateTime.SerializationFormat(
                    omit_zero_nanoseconds=False,
                ),
                http_types.LocalDateTime.SerializationFormat(
                    omit_zero_nanoseconds=False,
                    trim_nanoseconds=False,
                ),
            ):
                with self.subTest(format=dataclasses.asdict(fmt)):
                    dt_args = (800, 1, 1, 0, 0, 0, 0)
                    cypher_value = types.CypherDateTime(*dt_args)
                    http_value = http_types.LocalDateTime(*dt_args, fmt=fmt)
                    self._echo_session_run(
                        server,
                        cypher_value,
                        http_value,
                    )

    def test_duration(self):
        def min_field(module: int) -> int:
            return MIN_INT64 // module - (MIN_INT64 % module == 0) + 1

        max_months = MAX_INT64 // AVERAGE_SECONDS_IN_MONTH
        min_months = min_field(AVERAGE_SECONDS_IN_MONTH)
        max_weeks = MAX_INT64 // (AVERAGE_SECONDS_IN_DAY * 7)
        min_weeks = min_field(AVERAGE_SECONDS_IN_DAY * 7)
        max_days = MAX_INT64 // AVERAGE_SECONDS_IN_DAY
        min_days = min_field(AVERAGE_SECONDS_IN_DAY)
        max_hours = MAX_INT64 // 3600
        min_hours = min_field(3600)
        max_minutes = MAX_INT64 // 60
        min_minutes = min_field(60)
        max_seconds = MAX_INT64
        min_seconds = MIN_INT64

        with self.server() as server:
            for cypher_value, http_value in (
                (
                    types.CypherDuration(0, 0, 0, 0),
                    http_types.Duration(0, 0, 0, 0, 0, 0, 0, 0),
                ),
                (
                    types.CypherDuration(1, 2, 3, 4),
                    http_types.Duration(0, 1, 0, 2, 0, 0, 3, 4),
                ),
                (
                    types.CypherDuration(-1, -2, -3, -4),
                    http_types.Duration(0, -1, 0, -2, 0, 0, -3, -4),
                ),
                (
                    types.CypherDuration(1, -2, 3, 4),
                    http_types.Duration(0, 1, 0, -2, 0, 0, 3, 4),
                ),
                (
                    types.CypherDuration(-1, 2, -3, -4),
                    http_types.Duration(0, -1, 0, 2, 0, 0, -3, -4),
                ),
                (
                    types.CypherDuration(0, 0, 0, -999999999),
                    http_types.Duration(0, 0, 0, 0, 0, 0, 0, -999999999),
                ),
                (
                    types.CypherDuration(max_months, 0, 0, 0),
                    http_types.Duration(0, max_months, 0, 0, 0, 0, 0, 0),
                ),
                (
                    types.CypherDuration(min_months, 0, 0, 0),
                    http_types.Duration(0, min_months, 0, 0, 0, 0, 0, 0),
                ),
                (
                    types.CypherDuration(0, max_weeks * 7, 0, 0),
                    http_types.Duration(0, 0, max_weeks, 0, 0, 0, 0, 0),
                ),
                (
                    types.CypherDuration(0, min_weeks * 7, 0, 0),
                    http_types.Duration(0, 0, min_weeks, 0, 0, 0, 0, 0),
                ),
                (
                    types.CypherDuration(0, max_days, 0, 0),
                    http_types.Duration(0, 0, 0, max_days, 0, 0, 0, 0),
                ),
                (
                    types.CypherDuration(0, min_days, 0, 0),
                    http_types.Duration(0, 0, 0, min_days, 0, 0, 0, 0),
                ),
                (
                    types.CypherDuration(0, 0, max_hours * 3600, 0),
                    http_types.Duration(0, 0, 0, 0, max_hours, 0, 0, 0),
                ),
                (
                    types.CypherDuration(0, 0, min_hours * 3600, 0),
                    http_types.Duration(0, 0, 0, 0, min_hours, 0, 0, 0),
                ),
                (
                    types.CypherDuration(0, 0, max_minutes * 60, 0),
                    http_types.Duration(0, 0, 0, 0, 0, max_minutes, 0, 0),
                ),
                (
                    types.CypherDuration(0, 0, min_minutes * 60, 0),
                    http_types.Duration(0, 0, 0, 0, 0, min_minutes, 0, 0),
                ),
                (
                    types.CypherDuration(0, 0, max_seconds, 0),
                    http_types.Duration(0, 0, 0, 0, 0, 0, max_seconds, 0),
                ),
                (
                    types.CypherDuration(0, 0, min_seconds, 0),
                    http_types.Duration(0, 0, 0, 0, 0, 0, min_seconds, 0),
                ),
            ):
                with self.subTest(x=http_value):
                    self._echo_session_run(
                        server,
                        cypher_value,
                        http_value,
                    )

    def test_duration_formats(self):
        with self.server() as server:
            for http_value, fmt in (
                (
                    http_types.Duration(0, 0, 0, 0, 0, 0, 0, 0),
                    http_types.Duration.SerializationFormat(
                        omit_zero_years=False,
                        omit_zero_months=False,
                        omit_zero_weeks=False,
                        omit_zero_days=False,
                        omit_zero_hours=False,
                        omit_zero_minutes=False,
                        omit_zero_seconds=False,
                        omit_zero_nanoseconds=False,
                        trim_nanoseconds=False,
                    ),
                ),
                (
                    http_types.Duration(0, 0, 0, 0, 0, 0, 1, 234500000),
                    http_types.Duration.SerializationFormat(
                        decimal_point=",",
                    ),
                ),
            ):
                with self.subTest(fmt=dataclasses.asdict(http_value.fmt)):
                    http_value.fmt = fmt
                    http_value_normalized = http_value.normalize_compact()
                    cypher_value = types.CypherDuration(
                        months=http_value_normalized.months,
                        days=http_value_normalized.days,
                        seconds=http_value_normalized.seconds,
                        nanoseconds=http_value_normalized.nanoseconds,
                    )
                    self._echo_session_run(
                        server,
                        cypher_value,
                        http_value,
                    )
