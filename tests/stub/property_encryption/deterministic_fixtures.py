# These entries were initially generated with the .NET driver.

from dataclasses import dataclass

import nutkit.protocol as types

DETERMINISTIC_PROFILE_NAME = "deterministic"

DETERMINISTIC_KEK = bytes.fromhex(
    "4feaec7d8374cb75bd5045c079a2adaa"
    "7a7a1f3b16273fdd14b473f651398b5a"
)
DETERMINISTIC_ENCAPSULATION = bytes.fromhex(
    "e05287622c56ada2a11bb05c346b08c2"
    "c1fef76c4e0f8be89adb6193a6414318"
    "5a56c5daa1002eacd8bb2d557762b48e"
)
DETERMINISTIC_KEY_METADATA = {"iv": "pYQJeZ/fdg4ooz8I"}


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
            "63cc11fc4171fe16c17467bbca0418c2"
            "4041622487424f4f4c45414e0100a586"
            "6b65795f69648130826976cc0c000102"
            "030405060708090a0b83616164cc00d0"
            "126161645f70726f746f636f6c5f6d61"
            "6a6f7201d0126161645f70726f746f63"
            "6f6c5f6d696e6f7200"
        ),
    ),
    DeterministicFixture(
        value=types.CypherInt(32768),
        iv=bytes.fromhex(
            "0c0d0e0f1011121314151617"
        ),
        encrypted=bytes.fromhex(
            "01b6658d64657465726d696e69737469"
            "63cc157289ad84e2905a1a934cfb4fdd"
            "99946880fbb0ca1f87494e5445474552"
            "0100a5866b65795f69648130826976cc"
            "0c0c0d0e0f1011121314151617836161"
            "64cc00d0126161645f70726f746f636f"
            "6c5f6d616a6f7201d0126161645f7072"
            "6f746f636f6c5f6d696e6f7200"
        ),
    ),
    DeterministicFixture(
        value=types.CypherFloat(3.25),
        iv=bytes.fromhex(
            "18191a1b1c1d1e1f20212223"
        ),
        encrypted=bytes.fromhex(
            "01b6658d64657465726d696e69737469"
            "63cc19e3ecdce6cf2b3d75ab2b863a22"
            "4d16756c1ae96696fd287e8485464c4f"
            "41540100a5866b65795f696481308269"
            "76cc0c18191a1b1c1d1e1f2021222383"
            "616164cc00d0126161645f70726f746f"
            "636f6c5f6d616a6f7201d0126161645f"
            "70726f746f636f6c5f6d696e6f7200"
        ),
    ),
    DeterministicFixture(
        value=types.CypherString("hello world"),
        iv=bytes.fromhex(
            "2425262728292a2b2c2d2e2f"
        ),
        encrypted=bytes.fromhex(
            "01b6658d64657465726d696e69737469"
            "63cc1ca3ee14303c4684c2ec4db78ddc"
            "91f6291586b7927f19ac3aa142341e86"
            "535452494e470100a5866b65795f6964"
            "8130826976cc0c2425262728292a2b2c"
            "2d2e2f83616164cc00d0126161645f70"
            "726f746f636f6c5f6d616a6f7201d012"
            "6161645f70726f746f636f6c5f6d696e"
            "6f7200"
        ),
    ),
    DeterministicFixture(
        value=types.CypherBytes(b"\x00\x01\x02"),
        iv=bytes.fromhex(
            "303132333435363738393a3b"
        ),
        encrypted=bytes.fromhex(
            "01b6658d64657465726d696e69737469"
            "63cc153356122560582ac73ba05eeaef"
            "5293b7008512ade78542595445530100"
            "a5866b65795f69648130826976cc0c30"
            "3132333435363738393a3b83616164cc"
            "00d0126161645f70726f746f636f6c5f"
            "6d616a6f7201d0126161645f70726f74"
            "6f636f6c5f6d696e6f7200"
        ),
    ),
    DeterministicFixture(
        value=types.CypherList([types.CypherInt(1), types.CypherInt(2)]),
        iv=bytes.fromhex(
            "3c3d3e3f4041424344454647"
        ),
        encrypted=bytes.fromhex(
            "01b6658d64657465726d696e69737469"
            "63cc137f5458987009880d7077c665b4"
            "2f4735b83501844c4953540100a5866b"
            "65795f69648130826976cc0c3c3d3e3f"
            "404142434445464783616164cc00d012"
            "6161645f70726f746f636f6c5f6d616a"
            "6f7201d0126161645f70726f746f636f"
            "6c5f6d696e6f7200"
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
            "63cc1aa1c3405cd0e1b61e32045c2c58"
            "c2f76c8ba90da91422480a4413865354"
            "52494e470100a5866b65795f69648130"
            "826976cc0c48494a4b4c4d4e4f505152"
            "5383616164cc0786726f772d3432d012"
            "6161645f70726f746f636f6c5f6d616a"
            "6f7201d0126161645f70726f746f636f"
            "6c5f6d696e6f7200"
        ),
    ),
]
