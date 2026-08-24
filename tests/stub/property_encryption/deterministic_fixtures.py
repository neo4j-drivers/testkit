# These entries were initially generated with the .NET driver.

from dataclasses import dataclass

import nutkit.protocol as types

DETERMINISTIC_PROFILE_NAME = "deterministic"

DETERMINISTIC_KEK = bytes.fromhex(
    "f0de94eb5a2d4da6f17ea74b14e9e556"
    "d367cb22b053e01798aa2677bfcf5761"
)
DETERMINISTIC_ENCAPSULATION = bytes.fromhex(
    "9e1f562dee78c6c2d47f4378d2949774"
    "c3a56339b824abaf276c4ca7fcf5a8cd"
    "63976ae348104d6757b9e419bf9ea325"
)
DETERMINISTIC_KEY_METADATA = {"iv": "P02Pc7vInYIQ7k93"}
DETERMINISTIC_KEY_ID = "0"


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
            "63cc11877fe22670d0d3433e2a9c4dd5"
            "fd17994b87424f4f4c45414e0100a282"
            "6976cc0c000102030405060708090a0b"
            "866b65795f69648130"
        ),
    ),
    DeterministicFixture(
        value=types.CypherInt(32768),
        iv=bytes.fromhex(
            "0c0d0e0f1011121314151617"
        ),
        encrypted=bytes.fromhex(
            "01b6658d64657465726d696e69737469"
            "63cc153022e4a29e68285f6fddac8604"
            "5e26b63ba5da995087494e5445474552"
            "0100a2826976cc0c0c0d0e0f10111213"
            "14151617866b65795f69648130"
        ),
    ),
    DeterministicFixture(
        value=types.CypherFloat(3.25),
        iv=bytes.fromhex(
            "18191a1b1c1d1e1f20212223"
        ),
        encrypted=bytes.fromhex(
            "01b6658d64657465726d696e69737469"
            "63cc1959a942a76621fe2aa1f2d388ab"
            "4e91010e4b39b48520328e9585464c4f"
            "41540100a2826976cc0c18191a1b1c1d"
            "1e1f20212223866b65795f69648130"
        ),
    ),
    DeterministicFixture(
        value=types.CypherString("hello world"),
        iv=bytes.fromhex(
            "2425262728292a2b2c2d2e2f"
        ),
        encrypted=bytes.fromhex(
            "01b6658d64657465726d696e69737469"
            "63cc1c19b0e5f67ee23e78eb73899546"
            "4420a6fd3626fc052501e325cee12586"
            "535452494e470100a2826976cc0c2425"
            "262728292a2b2c2d2e2f866b65795f69"
            "648130"
        ),
    ),
    DeterministicFixture(
        value=types.CypherBytes(b"\x00\x01\x02"),
        iv=bytes.fromhex(
            "303132333435363738393a3b"
        ),
        encrypted=bytes.fromhex(
            "01b6658d64657465726d696e69737469"
            "63cc1529ea2acd117b82841138917fc8"
            "9cdf15cb4f0d809d8542595445530100"
            "a2826976cc0c30313233343536373839"
            "3a3b866b65795f69648130"
        ),
    ),
    DeterministicFixture(
        value=types.CypherList([types.CypherInt(1), types.CypherInt(2)]),
        iv=bytes.fromhex(
            "3c3d3e3f4041424344454647"
        ),
        encrypted=bytes.fromhex(
            "01b6658d64657465726d696e69737469"
            "63cc13b5011505031e789718bd92136f"
            "766baa191036844c4953540100a28269"
            "76cc0c3c3d3e3f404142434445464786"
            "6b65795f69648130"
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
            "63cc1a3a8af0d3820a0a549d75e42e59"
            "6a18ff85ee74fb51dce4bc0300865354"
            "52494e470100a583616164cc0786726f"
            "772d3432d0196161645f656e636f6469"
            "6e675f736368656d655f6d616a6f7201"
            "d0196161645f656e636f64696e675f73"
            "6368656d655f6d696e6f7200826976cc"
            "0c48494a4b4c4d4e4f50515253866b65"
            "795f69648130"
        ),
    ),
]
