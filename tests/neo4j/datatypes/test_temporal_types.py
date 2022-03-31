import datetime

import pytz

import nutkit.protocol as types
from tests.neo4j.datatypes._base import (
    _TestTypesBase,
    MAX_INT64,
    MIN_INT64,
)
from tests.shared import get_driver_name


class TestDataTypes(_TestTypesBase):

    required_features = (types.Feature.API_TYPE_TEMPORAL,)

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
                if get_driver_name() in ["python"]:
                    if (isinstance(val, types.CypherDateTime)
                            and val.utc_offset_s == 0):
                        self.skipTest(
                            "timezone library cannot tell the difference "
                            "between named UTC and 0s offset timezone"
                        )
                self._verify_can_echo(val)

    def test_should_echo_all_timezone_ids(self):
        tz_ids = (
            "Africa/Abidjan",
            "Africa/Accra",
            "Africa/Addis_Ababa",
            "Africa/Algiers",
            "Africa/Asmara",
            "Africa/Asmera",
            "Africa/Bamako",
            "Africa/Bangui",
            "Africa/Banjul",
            "Africa/Bissau",
            "Africa/Blantyre",
            "Africa/Brazzaville",
            "Africa/Bujumbura",
            "Africa/Cairo",
            "Africa/Casablanca",
            "Africa/Ceuta",
            "Africa/Conakry",
            "Africa/Dakar",
            "Africa/Dar_es_Salaam",
            "Africa/Djibouti",
            "Africa/Douala",
            "Africa/El_Aaiun",
            "Africa/Freetown",
            "Africa/Gaborone",
            "Africa/Harare",
            "Africa/Johannesburg",
            "Africa/Juba",
            "Africa/Kampala",
            "Africa/Khartoum",
            "Africa/Kigali",
            "Africa/Kinshasa",
            "Africa/Lagos",
            "Africa/Libreville",
            "Africa/Lome",
            "Africa/Luanda",
            "Africa/Lubumbashi",
            "Africa/Lusaka",
            "Africa/Malabo",
            "Africa/Maputo",
            "Africa/Maseru",
            "Africa/Mbabane",
            "Africa/Mogadishu",
            "Africa/Monrovia",
            "Africa/Nairobi",
            "Africa/Ndjamena",
            "Africa/Niamey",
            "Africa/Nouakchott",
            "Africa/Ouagadougou",
            "Africa/Porto-Novo",
            "Africa/Sao_Tome",
            "Africa/Timbuktu",
            "Africa/Tripoli",
            "Africa/Tunis",
            "Africa/Windhoek",
            "America/Adak",
            "America/Anchorage",
            "America/Anguilla",
            "America/Antigua",
            "America/Araguaina",
            "America/Argentina/Buenos_Aires",
            "America/Argentina/Catamarca",
            "America/Argentina/ComodRivadavia",
            "America/Argentina/Cordoba",
            "America/Argentina/Jujuy",
            "America/Argentina/La_Rioja",
            "America/Argentina/Mendoza",
            "America/Argentina/Rio_Gallegos",
            "America/Argentina/Salta",
            "America/Argentina/San_Juan",
            "America/Argentina/San_Luis",
            "America/Argentina/Tucuman",
            "America/Argentina/Ushuaia",
            "America/Aruba",
            "America/Asuncion",
            "America/Atikokan",
            "America/Atka",
            "America/Bahia",
            "America/Bahia_Banderas",
            "America/Barbados",
            "America/Belem",
            "America/Belize",
            "America/Blanc-Sablon",
            "America/Boa_Vista",
            "America/Bogota",
            "America/Boise",
            "America/Buenos_Aires",
            "America/Cambridge_Bay",
            "America/Campo_Grande",
            "America/Cancun",
            "America/Caracas",
            "America/Catamarca",
            "America/Cayenne",
            "America/Cayman",
            "America/Chicago",
            "America/Chihuahua",
            "America/Coral_Harbour",
            "America/Cordoba",
            "America/Costa_Rica",
            "America/Creston",
            "America/Cuiaba",
            "America/Curacao",
            "America/Danmarkshavn",
            "America/Dawson",
            "America/Dawson_Creek",
            "America/Denver",
            "America/Detroit",
            "America/Dominica",
            "America/Edmonton",
            "America/Eirunepe",
            "America/El_Salvador",
            "America/Ensenada",
            "America/Fort_Nelson",
            "America/Fort_Wayne",
            "America/Fortaleza",
            "America/Glace_Bay",
            "America/Godthab",
            "America/Goose_Bay",
            "America/Grand_Turk",
            "America/Grenada",
            "America/Guadeloupe",
            "America/Guatemala",
            "America/Guayaquil",
            "America/Guyana",
            "America/Halifax",
            "America/Havana",
            "America/Hermosillo",
            "America/Indiana/Indianapolis",
            "America/Indiana/Knox",
            "America/Indiana/Marengo",
            "America/Indiana/Petersburg",
            "America/Indiana/Tell_City",
            "America/Indiana/Vevay",
            "America/Indiana/Vincennes",
            "America/Indiana/Winamac",
            "America/Indianapolis",
            "America/Inuvik",
            "America/Iqaluit",
            "America/Jamaica",
            "America/Jujuy",
            "America/Juneau",
            "America/Kentucky/Louisville",
            "America/Kentucky/Monticello",
            "America/Knox_IN",
            "America/Kralendijk",
            "America/La_Paz",
            "America/Lima",
            "America/Los_Angeles",
            "America/Louisville",
            "America/Lower_Princes",
            "America/Maceio",
            "America/Managua",
            "America/Manaus",
            "America/Marigot",
            "America/Martinique",
            "America/Matamoros",
            "America/Mazatlan",
            "America/Mendoza",
            "America/Menominee",
            "America/Merida",
            "America/Metlakatla",
            "America/Mexico_City",
            "America/Miquelon",
            "America/Moncton",
            "America/Monterrey",
            "America/Montevideo",
            "America/Montreal",
            "America/Montserrat",
            "America/Nassau",
            "America/New_York",
            "America/Nipigon",
            "America/Nome",
            "America/Noronha",
            "America/North_Dakota/Beulah",
            "America/North_Dakota/Center",
            "America/North_Dakota/New_Salem",
            "America/Nuuk",
            "America/Ojinaga",
            "America/Panama",
            "America/Pangnirtung",
            "America/Paramaribo",
            "America/Phoenix",
            "America/Port-au-Prince",
            "America/Port_of_Spain",
            "America/Porto_Acre",
            "America/Porto_Velho",
            "America/Puerto_Rico",
            "America/Punta_Arenas",
            "America/Rainy_River",
            "America/Rankin_Inlet",
            "America/Recife",
            "America/Regina",
            "America/Resolute",
            "America/Rio_Branco",
            "America/Rosario",
            "America/Santa_Isabel",
            "America/Santarem",
            "America/Santiago",
            "America/Santo_Domingo",
            "America/Sao_Paulo",
            "America/Scoresbysund",
            "America/Shiprock",
            "America/Sitka",
            "America/St_Barthelemy",
            "America/St_Johns",
            "America/St_Kitts",
            "America/St_Lucia",
            "America/St_Thomas",
            "America/St_Vincent",
            "America/Swift_Current",
            "America/Tegucigalpa",
            "America/Thule",
            "America/Thunder_Bay",
            "America/Tijuana",
            "America/Toronto",
            "America/Tortola",
            "America/Vancouver",
            "America/Virgin",
            "America/Whitehorse",
            "America/Winnipeg",
            "America/Yakutat",
            "America/Yellowknife",
            "Antarctica/Casey",
            "Antarctica/Davis",
            "Antarctica/DumontDUrville",
            "Antarctica/Macquarie",
            "Antarctica/Mawson",
            "Antarctica/McMurdo",
            "Antarctica/Palmer",
            "Antarctica/Rothera",
            "Antarctica/South_Pole",
            "Antarctica/Syowa",
            "Antarctica/Troll",
            "Antarctica/Vostok",
            "Arctic/Longyearbyen",
            "Asia/Aden",
            "Asia/Almaty",
            "Asia/Amman",
            "Asia/Anadyr",
            "Asia/Aqtau",
            "Asia/Aqtobe",
            "Asia/Ashgabat",
            "Asia/Ashkhabad",
            "Asia/Atyrau",
            "Asia/Baghdad",
            "Asia/Bahrain",
            "Asia/Baku",
            "Asia/Bangkok",
            "Asia/Barnaul",
            "Asia/Beirut",
            "Asia/Bishkek",
            "Asia/Brunei",
            "Asia/Calcutta",
            "Asia/Chita",
            "Asia/Choibalsan",
            "Asia/Chongqing",
            "Asia/Chungking",
            "Asia/Colombo",
            "Asia/Dacca",
            "Asia/Damascus",
            "Asia/Dhaka",
            "Asia/Dili",
            "Asia/Dubai",
            "Asia/Dushanbe",
            "Asia/Famagusta",
            "Asia/Gaza",
            "Asia/Harbin",
            "Asia/Hebron",
            "Asia/Ho_Chi_Minh",
            "Asia/Hong_Kong",
            "Asia/Hovd",
            "Asia/Irkutsk",
            "Asia/Istanbul",
            "Asia/Jakarta",
            "Asia/Jayapura",
            "Asia/Jerusalem",
            "Asia/Kabul",
            "Asia/Kamchatka",
            "Asia/Karachi",
            "Asia/Kashgar",
            "Asia/Kathmandu",
            "Asia/Katmandu",
            "Asia/Khandyga",
            "Asia/Kolkata",
            "Asia/Krasnoyarsk",
            "Asia/Kuala_Lumpur",
            "Asia/Kuching",
            "Asia/Kuwait",
            "Asia/Macao",
            "Asia/Macau",
            "Asia/Magadan",
            "Asia/Makassar",
            "Asia/Manila",
            "Asia/Muscat",
            "Asia/Nicosia",
            "Asia/Novokuznetsk",
            "Asia/Novosibirsk",
            "Asia/Omsk",
            "Asia/Oral",
            "Asia/Phnom_Penh",
            "Asia/Pontianak",
            "Asia/Pyongyang",
            "Asia/Qatar",
            "Asia/Qostanay",
            "Asia/Qyzylorda",
            "Asia/Rangoon",
            "Asia/Riyadh",
            "Asia/Saigon",
            "Asia/Sakhalin",
            "Asia/Samarkand",
            "Asia/Seoul",
            "Asia/Shanghai",
            "Asia/Singapore",
            "Asia/Srednekolymsk",
            "Asia/Taipei",
            "Asia/Tashkent",
            "Asia/Tbilisi",
            "Asia/Tehran",
            "Asia/Tel_Aviv",
            "Asia/Thimbu",
            "Asia/Thimphu",
            "Asia/Tokyo",
            "Asia/Tomsk",
            "Asia/Ujung_Pandang",
            "Asia/Ulaanbaatar",
            "Asia/Ulan_Bator",
            "Asia/Urumqi",
            "Asia/Ust-Nera",
            "Asia/Vientiane",
            "Asia/Vladivostok",
            "Asia/Yakutsk",
            "Asia/Yangon",
            "Asia/Yekaterinburg",
            "Asia/Yerevan",
            "Atlantic/Azores",
            "Atlantic/Bermuda",
            "Atlantic/Canary",
            "Atlantic/Cape_Verde",
            "Atlantic/Faeroe",
            "Atlantic/Faroe",
            "Atlantic/Jan_Mayen",
            "Atlantic/Madeira",
            "Atlantic/Reykjavik",
            "Atlantic/South_Georgia",
            "Atlantic/St_Helena",
            "Atlantic/Stanley",
            "Australia/ACT",
            "Australia/Adelaide",
            "Australia/Brisbane",
            "Australia/Broken_Hill",
            "Australia/Canberra",
            "Australia/Currie",
            "Australia/Darwin",
            "Australia/Eucla",
            "Australia/Hobart",
            "Australia/LHI",
            "Australia/Lindeman",
            "Australia/Lord_Howe",
            "Australia/Melbourne",
            "Australia/North",
            "Australia/NSW",
            "Australia/Perth",
            "Australia/Queensland",
            "Australia/South",
            "Australia/Sydney",
            "Australia/Tasmania",
            "Australia/Victoria",
            "Australia/West",
            "Australia/Yancowinna",
            "Brazil/Acre",
            "Brazil/DeNoronha",
            "Brazil/East",
            "Brazil/West",
            "Canada/Atlantic",
            "Canada/Central",
            "Canada/Eastern",
            "Canada/Mountain",
            "Canada/Newfoundland",
            "Canada/Pacific",
            "Canada/Saskatchewan",
            "Canada/Yukon",
            "CET",
            "Chile/Continental",
            "Chile/EasterIsland",
            "CST6CDT",
            "Cuba",
            "EET",
            "Egypt",
            "Eire",
            # "EST",  # unsupported by cypher
            "EST5EDT",
            "Etc/GMT",
            "Etc/GMT0",
            "Etc/GMT+0",
            "Etc/GMT+1",
            "Etc/GMT+2",
            "Etc/GMT+3",
            "Etc/GMT+4",
            "Etc/GMT+5",
            "Etc/GMT+6",
            "Etc/GMT+7",
            "Etc/GMT+8",
            "Etc/GMT+9",
            "Etc/GMT+10",
            "Etc/GMT+11",
            "Etc/GMT+12",
            "Etc/GMT-0",
            "Etc/GMT-1",
            "Etc/GMT-2",
            "Etc/GMT-3",
            "Etc/GMT-4",
            "Etc/GMT-5",
            "Etc/GMT-6",
            "Etc/GMT-7",
            "Etc/GMT-8",
            "Etc/GMT-9",
            "Etc/GMT-10",
            "Etc/GMT-11",
            "Etc/GMT-12",
            "Etc/GMT-13",
            "Etc/GMT-14",
            "Etc/Greenwich",
            "Etc/UCT",
            "Etc/Universal",
            "Etc/UTC",
            "Etc/Zulu",
            "Europe/Amsterdam",
            "Europe/Andorra",
            "Europe/Astrakhan",
            "Europe/Athens",
            "Europe/Belfast",
            "Europe/Belgrade",
            "Europe/Berlin",
            "Europe/Bratislava",
            "Europe/Brussels",
            "Europe/Bucharest",
            "Europe/Budapest",
            "Europe/Busingen",
            "Europe/Chisinau",
            "Europe/Copenhagen",
            "Europe/Dublin",
            "Europe/Gibraltar",
            "Europe/Guernsey",
            "Europe/Helsinki",
            "Europe/Isle_of_Man",
            "Europe/Istanbul",
            "Europe/Jersey",
            "Europe/Kaliningrad",
            "Europe/Kiev",
            "Europe/Kirov",
            "Europe/Lisbon",
            "Europe/Ljubljana",
            "Europe/London",
            "Europe/Luxembourg",
            "Europe/Madrid",
            "Europe/Malta",
            "Europe/Mariehamn",
            "Europe/Minsk",
            "Europe/Monaco",
            "Europe/Moscow",
            "Europe/Nicosia",
            "Europe/Oslo",
            "Europe/Paris",
            "Europe/Podgorica",
            "Europe/Prague",
            "Europe/Riga",
            "Europe/Rome",
            "Europe/Samara",
            "Europe/San_Marino",
            "Europe/Sarajevo",
            "Europe/Saratov",
            "Europe/Simferopol",
            "Europe/Skopje",
            "Europe/Sofia",
            "Europe/Stockholm",
            "Europe/Tallinn",
            "Europe/Tirane",
            "Europe/Tiraspol",
            "Europe/Ulyanovsk",
            "Europe/Uzhgorod",
            "Europe/Vaduz",
            "Europe/Vatican",
            "Europe/Vienna",
            "Europe/Vilnius",
            "Europe/Volgograd",
            "Europe/Warsaw",
            "Europe/Zagreb",
            "Europe/Zaporozhye",
            "Europe/Zurich",
            "GB",
            "GB-Eire",
            "GMT",
            "GMT0",
            "GMT+0",
            "GMT-0",
            "Greenwich",
            "Hongkong",
            # "HST",  # unsupported by cypher
            "Iceland",
            "Indian/Antananarivo",
            "Indian/Chagos",
            "Indian/Christmas",
            "Indian/Cocos",
            "Indian/Comoro",
            "Indian/Kerguelen",
            "Indian/Mahe",
            "Indian/Maldives",
            "Indian/Mauritius",
            "Indian/Mayotte",
            "Indian/Reunion",
            "Iran",
            "Israel",
            "Jamaica",
            "Japan",
            "Kwajalein",
            "Libya",
            "MET",
            "Mexico/BajaNorte",
            "Mexico/BajaSur",
            "Mexico/General",
            # "MST",  # unsupported by cypher
            "MST7MDT",
            "Navajo",
            "NZ",
            "NZ-CHAT",
            "Pacific/Apia",
            "Pacific/Auckland",
            "Pacific/Bougainville",
            "Pacific/Chatham",
            "Pacific/Chuuk",
            "Pacific/Easter",
            "Pacific/Efate",
            "Pacific/Enderbury",
            "Pacific/Fakaofo",
            "Pacific/Fiji",
            "Pacific/Funafuti",
            "Pacific/Galapagos",
            "Pacific/Gambier",
            "Pacific/Guadalcanal",
            "Pacific/Guam",
            "Pacific/Honolulu",
            "Pacific/Johnston",
            # "Pacific/Kanton",  # unsupported by cypher
            "Pacific/Kiritimati",
            "Pacific/Kosrae",
            "Pacific/Kwajalein",
            "Pacific/Majuro",
            "Pacific/Marquesas",
            "Pacific/Midway",
            "Pacific/Nauru",
            "Pacific/Niue",
            "Pacific/Norfolk",
            "Pacific/Noumea",
            "Pacific/Pago_Pago",
            "Pacific/Palau",
            "Pacific/Pitcairn",
            "Pacific/Pohnpei",
            "Pacific/Ponape",
            "Pacific/Port_Moresby",
            "Pacific/Rarotonga",
            "Pacific/Saipan",
            "Pacific/Samoa",
            "Pacific/Tahiti",
            "Pacific/Tarawa",
            "Pacific/Tongatapu",
            "Pacific/Truk",
            "Pacific/Wake",
            "Pacific/Wallis",
            "Pacific/Yap",
            "Poland",
            "Portugal",
            "PRC",
            "PST8PDT",
            # "ROC",  # unsupported by cypher
            "ROK",
            "Singapore",
            "Turkey",
            "UCT",
            "Universal",
            "US/Alaska",
            "US/Aleutian",
            "US/Arizona",
            "US/Central",
            "US/East-Indiana",
            "US/Eastern",
            "US/Hawaii",
            "US/Indiana-Starke",
            "US/Michigan",
            "US/Mountain",
            "US/Pacific",
            "US/Samoa",
            "UTC",
            "W-SU",
            "WET",
            "Zulu",
        )

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
        for tz_id in tz_ids:
            for time in times:
                with self.subTest(tz_id=tz_id, time=time):

                    if get_driver_name() in ["python"]:
                        if tz_id in ["GMT+0", "GMT-0"]:
                            self.skipTest("timezone library turns GTM+0 and "
                                          "GMT-1 into GMT")

                    # FIXME: while there is a bug in the bolt protocol that
                    #        makes it incapable of representing datetimes with
                    #        timezone ids when there is ambiguity, we will
                    #        avoid those.
                    tz = pytz.timezone(tz_id)
                    naive_dt = datetime.datetime(*time[:-1])
                    dst_local_dt = tz.localize(naive_dt, is_dst=True)
                    no_dst_local_dt = tz.localize(naive_dt, is_dst=False)
                    while dst_local_dt != no_dst_local_dt:
                        naive_dt += datetime.timedelta(hours=1)
                        dst_local_dt = tz.localize(naive_dt, is_dst=True)
                        no_dst_local_dt = tz.localize(naive_dt, is_dst=False)

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
                    self._verify_can_echo(dt)

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
        values = self._read_query_values(
            "CYPHER runtime=interpreted WITH $x AS x "
            "RETURN [x.year, x.month, x.day, x.hour, x.minute, x.second, "
            "x.nanosecond, x.offset]",
            params={"x": types.CypherDateTime(2022, 3, 30, 13, 24, 34,
                                              699546224, utc_offset_s=-5520)}
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
                                     utc_offset_s=0, timezone_id="UTC")
            ),
            (
                "datetime('1976-06-13T12:34:56.999888777')",
                types.CypherDateTime(1976, 6, 13, 12, 34, 56, 999888777,
                                     utc_offset_s=0, timezone_id="UTC")
            ),
            (
                "datetime('1976-06-13T12:34:56.999888777-05:00')",
                types.CypherDateTime(1976, 6, 13, 12, 34, 56, 999888777,
                                     utc_offset_s=-18000)
            ),
            (
                "datetime('1976-06-13T12:34:56.789012345[Europe/London]')",
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
                values = self._read_query_values(f"RETURN {s}")
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
