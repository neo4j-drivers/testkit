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

# A 16-byte (AES-128) key wrapped under DETERMINISTIC_KEK.
AES_128_KEY_ENCAPSULATION = bytes.fromhex(
    "b5e4a863e58f50a970e0a289ea8cf2d8"
    "14bd2c6ce3a663382b1fa499852aff6d"
)
AES_128_KEY_METADATA = {"iv": "oKGio6Slpqeoqaqr"}
AES_128_KEY_ID = "testkit-aes-128-key"


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
    DeterministicFixture(
        value=types.CypherString("aad-bound"),
        iv=bytes.fromhex(
            "606162636465666768696a6b"
        ),
        aad=types.CypherBool(True),
        encrypted=bytes.fromhex(
            "01b86588454e56454c4f5045018d6465"
            "7465726d696e6973746963cc1a68449e"
            "b60c146b6d670344a256a9e4099a7307"
            "df0364e05cc77786535452494e470100"
            "a583616164cc01c3d0196161645f656e"
            "636f64696e675f736368656d655f6d61"
            "6a6f7201d0196161645f656e636f6469"
            "6e675f736368656d655f6d696e6f7200"
            "826976cc0c606162636465666768696a"
            "6b866b65795f69648b746573746b6974"
            "2d6b6579"
        ),
    ),
    DeterministicFixture(
        value=types.CypherString("aad-bound"),
        iv=bytes.fromhex(
            "6c6d6e6f7071727374757677"
        ),
        aad=types.CypherInt(1234567890123),
        encrypted=bytes.fromhex(
            "01b86588454e56454c4f5045018d6465"
            "7465726d696e6973746963cc1abc468b"
            "61966afda48be2eec3a5bb06fc5b1d7d"
            "0df62c997b255086535452494e470100"
            "a583616164cc09cb0000011f71fb04cb"
            "d0196161645f656e636f64696e675f73"
            "6368656d655f6d616a6f7201d0196161"
            "645f656e636f64696e675f736368656d"
            "655f6d696e6f7200826976cc0c6c6d6e"
            "6f7071727374757677866b65795f6964"
            "8b746573746b69742d6b6579"
        ),
    ),
    DeterministicFixture(
        value=types.CypherString("aad-bound"),
        iv=bytes.fromhex(
            "78797a7b7c7d7e7f80818283"
        ),
        aad=types.CypherBytes(b"\x00\x01\x02"),
        encrypted=bytes.fromhex(
            "01b86588454e56454c4f5045018d6465"
            "7465726d696e6973746963cc1a777608"
            "529a756d2c34e2482f306ec900ac962a"
            "0cd5a3e6f72aee86535452494e470100"
            "a583616164cc05cc03000102d0196161"
            "645f656e636f64696e675f736368656d"
            "655f6d616a6f7201d0196161645f656e"
            "636f64696e675f736368656d655f6d69"
            "6e6f7200826976cc0c78797a7b7c7d7e"
            "7f80818283866b65795f69648b746573"
            "746b69742d6b6579"
        ),
    ),
    DeterministicFixture(
        value=types.CypherString("aad-bound"),
        iv=bytes.fromhex(
            "8485868788898a8b8c8d8e8f"
        ),
        aad=types.CypherDate(2026, 10, 6),
        encrypted=bytes.fromhex(
            "01b86588454e56454c4f5045018d6465"
            "7465726d696e6973746963cc1a1086f9"
            "55d3c3ad3f982437213c2ece4758c0ff"
            "151aadf12592e086535452494e470100"
            "a583616164cc05b144c950fcd0196161"
            "645f656e636f64696e675f736368656d"
            "655f6d616a6f7201d0196161645f656e"
            "636f64696e675f736368656d655f6d69"
            "6e6f7200826976cc0c8485868788898a"
            "8b8c8d8e8f866b65795f69648b746573"
            "746b69742d6b6579"
        ),
    ),
    DeterministicFixture(
        value=types.CypherString("aad-bound"),
        iv=bytes.fromhex(
            "909192939495969798999a9b"
        ),
        aad=types.CypherTime(12, 34, 56, 789000000),
        encrypted=bytes.fromhex(
            "01b86588454e56454c4f5045018d6465"
            "7465726d696e6973746963cc1a4304ce"
            "4b10cba5c032fecf3599360107901a8d"
            "e44d44511e537386535452494e470100"
            "a583616164cc0bb174cb000029327b04"
            "8f40d0196161645f656e636f64696e67"
            "5f736368656d655f6d616a6f7201d019"
            "6161645f656e636f64696e675f736368"
            "656d655f6d696e6f7200826976cc0c90"
            "9192939495969798999a9b866b65795f"
            "69648b746573746b69742d6b6579"
        ),
    ),
    DeterministicFixture(
        value=types.CypherString("aad-bound"),
        iv=bytes.fromhex(
            "9c9d9e9fa0a1a2a3a4a5a6a7"
        ),
        aad=types.CypherTime(12, 34, 56, 789000000, utc_offset_s=3600),
        encrypted=bytes.fromhex(
            "01b86588454e56454c4f5045018d6465"
            "7465726d696e6973746963cc1a942bcc"
            "373cfb7819535c79e6f4c9a7b68479c2"
            "44c000162f004d86535452494e470100"
            "a583616164cc0eb254cb000029327b04"
            "8f40c90e10d0196161645f656e636f64"
            "696e675f736368656d655f6d616a6f72"
            "01d0196161645f656e636f64696e675f"
            "736368656d655f6d696e6f7200826976"
            "cc0c9c9d9e9fa0a1a2a3a4a5a6a7866b"
            "65795f69648b746573746b69742d6b65"
            "79"
        ),
    ),
    DeterministicFixture(
        value=types.CypherString("aad-bound"),
        iv=bytes.fromhex(
            "a8a9aaabacadaeafb0b1b2b3"
        ),
        aad=types.CypherPoint("cartesian", 1.5, 2.5),
        encrypted=bytes.fromhex(
            "01b86588454e56454c4f5045018d6465"
            "7465726d696e6973746963cc1a8d9f4a"
            "432d97cb8d12d6fce3788d1c42ae3d5a"
            "c5f4705672687086535452494e470100"
            "a583616164cc17b358c91c23c13ff800"
            "0000000000c14004000000000000d019"
            "6161645f656e636f64696e675f736368"
            "656d655f6d616a6f7201d0196161645f"
            "656e636f64696e675f736368656d655f"
            "6d696e6f7200826976cc0ca8a9aaabac"
            "adaeafb0b1b2b3866b65795f69648b74"
            "6573746b69742d6b6579"
        ),
    ),
    DeterministicFixture(
        value=types.CypherString("aad-bound"),
        iv=bytes.fromhex(
            "b4b5b6b7b8b9babbbcbdbebf"
        ),
        aad=types.CypherPoint("wgs84", 12.5, 56.25, 100.0),
        encrypted=bytes.fromhex(
            "01b86588454e56454c4f5045018d6465"
            "7465726d696e6973746963cc1aac05ed"
            "12902f11af147733a4ae690cd4c59c75"
            "8f2aa9a5d79d8086535452494e470100"
            "a583616164cc20b459c91373c1402900"
            "0000000000c1404c200000000000c140"
            "59000000000000d0196161645f656e63"
            "6f64696e675f736368656d655f6d616a"
            "6f7201d0196161645f656e636f64696e"
            "675f736368656d655f6d696e6f720082"
            "6976cc0cb4b5b6b7b8b9babbbcbdbebf"
            "866b65795f69648b746573746b69742d"
            "6b6579"
        ),
    ),
    DeterministicFixture(
        value=types.CypherString("aad-bound"),
        iv=bytes.fromhex(
            "c0c1c2c3c4c5c6c7c8c9cacb"
        ),
        aad=types.CypherUUID("8e1f2a6c-3b4d-4e5f-8a9b-0c1d2e3f4a5b"),
        encrypted=bytes.fromhex(
            "01b86588454e56454c4f5045018d6465"
            "7465726d696e6973746963cc1adfc267"
            "b81d8652836828c8554dcb7863a39b5d"
            "785c0b84cb172686535452494e470100"
            "a583616164cc11e08e1f2a6c3b4d4e5f"
            "8a9b0c1d2e3f4a5bd0196161645f656e"
            "636f64696e675f736368656d655f6d61"
            "6a6f7201d0196161645f656e636f6469"
            "6e675f736368656d655f6d696e6f7200"
            "826976cc0cc0c1c2c3c4c5c6c7c8c9ca"
            "cb866b65795f69648b746573746b6974"
            "2d6b6579"
        ),
    ),
]
