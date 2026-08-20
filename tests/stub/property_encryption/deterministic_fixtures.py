# These entries were initially generated with the .NET driver.

from dataclasses import dataclass

import nutkit.protocol as types

DETERMINISTIC_PROFILE_NAME = "deterministic"

DETERMINISTIC_KEK = bytes.fromhex(
    "a5be5a4e954ce4bad98c9fda44d6888c"
    "26e811b64daffec8918676f3ebf6220e"
)
DETERMINISTIC_ENCAPSULATION = bytes.fromhex(
    "f1b17045c1163d57cd611a26149439a8"
    "9ba087d37a4d4aa77dc6087e06c6f7fd"
    "eac9827f50e93a5361f6d5923bf2923b"
)
DETERMINISTIC_KEY_METADATA = {"iv": "U81P4B2CvQt2ykg3"}


@dataclass(frozen=True)
class DeterministicFixture:
    value: object
    iv: bytes
    encrypted: bytes
    aad: object = None


DETERMINISTIC_TEST_CASES = [
    DeterministicFixture(
        value=types.CypherBool(True),
        iv=bytes.fromhex(
            "000102030405060708090a0b"
        ),
        encrypted=bytes.fromhex(
            "01b6658d64657465726d696e69737469"
            "63cc11aaf92b0356bb039bb881e09c11"
            "6a9ef99687424f4f4c45414e0100a286"
            "6b65795f69648130826976cc0c000102"
            "030405060708090a0b"
        ),
    ),
    DeterministicFixture(
        value=types.CypherInt(32768),
        iv=bytes.fromhex(
            "0c0d0e0f1011121314151617"
        ),
        encrypted=bytes.fromhex(
            "01b6658d64657465726d696e69737469"
            "63cc150eb3e58bb9d7ffec21f8dbb7ec"
            "c8d41a9d0fd4b4b787494e5445474552"
            "0100a2866b65795f69648130826976cc"
            "0c0c0d0e0f1011121314151617"
        ),
    ),
    DeterministicFixture(
        value=types.CypherFloat(3.25),
        iv=bytes.fromhex(
            "18191a1b1c1d1e1f20212223"
        ),
        encrypted=bytes.fromhex(
            "01b6658d64657465726d696e69737469"
            "63cc19a835184ebf41b0e024656da074"
            "b80d02379b4fd866290068eb85464c4f"
            "41540100a2866b65795f696481308269"
            "76cc0c18191a1b1c1d1e1f20212223"
        ),
    ),
    DeterministicFixture(
        value=types.CypherString("hello world"),
        iv=bytes.fromhex(
            "2425262728292a2b2c2d2e2f"
        ),
        encrypted=bytes.fromhex(
            "01b6658d64657465726d696e69737469"
            "63cc1c46d9914fd25236d444f2a0f59e"
            "a321611003d9b08f79acaeb1975e6a86"
            "535452494e470100a2866b65795f6964"
            "8130826976cc0c2425262728292a2b2c"
            "2d2e2f"
        ),
    ),
    DeterministicFixture(
        value=types.CypherBytes(b"\x00\x01\x02"),
        iv=bytes.fromhex(
            "303132333435363738393a3b"
        ),
        encrypted=bytes.fromhex(
            "01b6658d64657465726d696e69737469"
            "63cc1585b226048348bcbc289d6e46cf"
            "0d7f1fb9d0e105188542595445530100"
            "a2866b65795f69648130826976cc0c30"
            "3132333435363738393a3b"
        ),
    ),
    DeterministicFixture(
        value=types.CypherList([types.CypherInt(1), types.CypherInt(2)]),
        iv=bytes.fromhex(
            "3c3d3e3f4041424344454647"
        ),
        encrypted=bytes.fromhex(
            "01b6658d64657465726d696e69737469"
            "63cc1340f23b075078ae0cceccacb3e5"
            "1073a65227ef844c4953540100a2866b"
            "65795f69648130826976cc0c3c3d3e3f"
            "4041424344454647"
        ),
    ),
    DeterministicFixture(
        value=types.CypherString("aad-bound"),
        iv=bytes.fromhex(
            "48494a4b4c4d4e4f50515253"
        ),
        aad=types.CypherString("row-42"),
        encrypted=bytes.fromhex(
            "01b6658d64657465726d696e69737469"
            "63cc1ae485f081fdbbc772219eb19708"
            "193afbc72f29d7e958caf65f1f865354"
            "52494e470100a5866b65795f69648130"
            "826976cc0c48494a4b4c4d4e4f505152"
            "5383616164cc0786726f772d3432d019"
            "6161645f656e636f64696e675f736368"
            "656d655f6d616a6f7201d0196161645f"
            "656e636f64696e675f736368656d655f"
            "6d696e6f7200"
        ),
    ),
]
