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
DETERMINISTIC_KEY_ID = "testkit-key"


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
            "01b86588454e56454c4f5045018d6465"
            "7465726d696e6973746963cc11766afe"
            "8a94ffb2fd0e0bc10caed2471a028742"
            "4f4f4c45414e0100a2826976cc0c0001"
            "02030405060708090a0b866b65795f69"
            "648b746573746b69742d6b6579"
        ),
    ),
    DeterministicFixture(
        value=types.CypherInt(32768),
        iv=bytes.fromhex(
            "0c0d0e0f1011121314151617"
        ),
        encrypted=bytes.fromhex(
            "01b86588454e56454c4f5045018d6465"
            "7465726d696e6973746963cc1569a784"
            "263a80ca0892009f92ad16223f54f9ea"
            "d06187494e54454745520100a2826976"
            "cc0c0c0d0e0f1011121314151617866b"
            "65795f69648b746573746b69742d6b65"
            "79"
        ),
    ),
    DeterministicFixture(
        value=types.CypherFloat(3.25),
        iv=bytes.fromhex(
            "18191a1b1c1d1e1f20212223"
        ),
        encrypted=bytes.fromhex(
            "01b86588454e56454c4f5045018d6465"
            "7465726d696e6973746963cc1992e03a"
            "fb3af69544000a301a57d5f17255f243"
            "10a3dabb5e6885464c4f41540100a282"
            "6976cc0c18191a1b1c1d1e1f20212223"
            "866b65795f69648b746573746b69742d"
            "6b6579"
        ),
    ),
    DeterministicFixture(
        value=types.CypherString("hello world"),
        iv=bytes.fromhex(
            "2425262728292a2b2c2d2e2f"
        ),
        encrypted=bytes.fromhex(
            "01b86588454e56454c4f5045018d6465"
            "7465726d696e6973746963cc1c1c78d1"
            "da0e71dc01f2a5d683cb925ef41e615f"
            "6258c77f6ff2dda96486535452494e47"
            "0100a2826976cc0c2425262728292a2b"
            "2c2d2e2f866b65795f69648b74657374"
            "6b69742d6b6579"
        ),
    ),
    DeterministicFixture(
        value=types.CypherBytes(b"\x00\x01\x02"),
        iv=bytes.fromhex(
            "303132333435363738393a3b"
        ),
        encrypted=bytes.fromhex(
            "01b86588454e56454c4f5045018d6465"
            "7465726d696e6973746963cc15e2719b"
            "8ded9e86fe2871fe9ee51e3730acd0ce"
            "9e118542595445530100a2826976cc0c"
            "303132333435363738393a3b866b6579"
            "5f69648b746573746b69742d6b6579"
        ),
    ),
    DeterministicFixture(
        value=types.CypherList([types.CypherInt(1), types.CypherInt(2)]),
        iv=bytes.fromhex(
            "3c3d3e3f4041424344454647"
        ),
        encrypted=bytes.fromhex(
            "01b86588454e56454c4f5045018d6465"
            "7465726d696e6973746963cc131a8297"
            "1faa128ae4267f07af8a7de4a80728f6"
            "844c4953540100a2826976cc0c3c3d3e"
            "3f4041424344454647866b65795f6964"
            "8b746573746b69742d6b6579"
        ),
    ),
    DeterministicFixture(
        value=types.CypherString("aad-bound"),
        iv=bytes.fromhex(
            "48494a4b4c4d4e4f50515253"
        ),
        aad=types.CypherString("row-42"),
        encrypted=bytes.fromhex(
            "01b86588454e56454c4f5045018d6465"
            "7465726d696e6973746963cc1a9e19aa"
            "f51fbb711fdeb241272be57efcd076c5"
            "6200e29a4e44da86535452494e470100"
            "a583616164cc0786726f772d3432d019"
            "6161645f656e636f64696e675f736368"
            "656d655f6d616a6f7201d0196161645f"
            "656e636f64696e675f736368656d655f"
            "6d696e6f7200826976cc0c48494a4b4c"
            "4d4e4f50515253866b65795f69648b74"
            "6573746b69742d6b6579"
        ),
    ),
    DeterministicFixture(
        value=types.CypherNull(),
        iv=bytes.fromhex(
            "5455565758595a5b5c5d5e5f"
        ),
        encrypted=bytes.fromhex(
            "01b86588454e56454c4f5045018d6465"
            "7465726d696e6973746963cc11c56a32"
            "a2d5ec6f489dadae9de9852db097844e"
            "554c4c0100a2826976cc0c5455565758"
            "595a5b5c5d5e5f866b65795f69648b74"
            "6573746b69742d6b6579"
        ),
    ),
]
