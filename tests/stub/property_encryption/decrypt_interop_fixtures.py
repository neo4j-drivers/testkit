"""
Shared cross-driver fixture list for the decrypt-interop check.

Each entry was produced by one driver team's own implementation, using
generate_decrypt_interop_fixture.py against their own backend. Every driver's
test suite decrypts every entry here, proving it can read values encrypted by
every other driver.

Add your own entry by running that script and pasting its output below.
"""

from dataclasses import dataclass

import nutkit.protocol as types

# don't change this or the interop decryption test will break
INTEROP_PROFILE_NAME = "interop"


@dataclass(frozen=True)
class DecryptInteropFixture:
    driver: str
    kek: bytes
    encapsulation: bytes
    metadata: dict
    encrypted: bytes
    value: object


DECRYPT_INTEROP_TEST_CASES = [
    DecryptInteropFixture(
        driver="dotnet",
        kek=bytes.fromhex(
            "1974e1620d8d540ffba202d9eeb616f4"
            "266a731f3eadd3637f25c71bb96ad024"
        ),
        encapsulation=bytes.fromhex(
            "014603135847721b9f3a1b1089ec98712"
            "1e03c8d703df8c35b7ca95f8893c84f6f"
            "e23c661aae26ef78bc9b1b6bb06905"
        ),
        metadata={"iv": "nJvPChdeMDkE/FDM"},
        encrypted=bytes.fromhex(
            "01b66587696e7465726f70cc24eab4839"
            "fe33026669beb1413b7a3581cb32edae0"
            "1e2c94e41e6e4ed741e6302df4da42208"
            "6535452494e470100a5866b65795f6964"
            "8130826976cc0ca3feff5e0278a951c49"
            "0a11483616164cc00d0126161645f7072"
            "6f746f636f6c5f6d616a6f7201d012616"
            "1645f70726f746f636f6c5f6d696e6f72"
            "00"
        ),
        value=types.CypherString("hello from dotnet!"),
    ),
    DecryptInteropFixture(
        driver="javascript",
        kek=bytes.fromhex(
            "ac4828b563d2dd626d34214c3dcd8168"
            "ed77c5ed9620b383e4b63665286302f9"
        ),
        encapsulation=bytes.fromhex(
            "4320112f90e2228f4dabffdc82c904bff"
            "f33eb70b4b162354474cb6a389717e9df"
            "e3077d9255913ee6d0e30f2a9f55f0"
        ),
        metadata={"iv": "lL9ga7QC9WWcKXH6"},
        encrypted=bytes.fromhex(
            "01b66587696e7465726f70cc28e4d50c7"
            "76bdf240ccb0afd823376dfda1b55ba68"
            "2601933fe5fc15a9276dcdd20e2cda276"
            "8236a1686535452494e470100a2826976"
            "cc0c343f2b095b3385d0d7adf455866b6"
            "5795f69648130"
        ),
        value=types.CypherString("hello from javascript!"),
    ),
]
