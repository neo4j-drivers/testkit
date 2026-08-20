"""
Generates an entry for the fixture list in decrypt_interop_fixtures.py.

Make sure the backend is running, then run the script. The script connects to
the backend and pretends to be starting a test, and creates a driver. The
connection details are nonsense because it's not actually going to try to
connect to a database, just using the encryption property.

If your backend is passing the encryption stub tests then this script will
work, and it will print out an entry for you to paste into the FIXTURES list
in decrypt_interop_fixtures.py. The actual test in test_property_encryption.py
picks it up from there.

Run as a module from the repo root with your backend already listening on
:9876:

    cd ~/dev/testkit # repo root
    python3 -m tests.stub.property_encryption.generate_decrypt_interop_fixture

"""

import secrets

import nutkit.protocol as types
from nutkit.frontend import Driver
from tests.shared import (
    get_driver_name,
    new_backend,
)
from tests.stub.property_encryption.decrypt_interop_fixtures import (
    INTEROP_PROFILE_NAME,
)


def main():
    backend = new_backend()
    try:
        backend.send_and_receive(types.GetFeatures())
        backend.send_and_receive(
            types.StartTest("generate_decrypt_interop_fixture")
        )

        string_to_encrypt = f"hello from {get_driver_name()}!"
        kek = secrets.token_bytes(32)
        value = types.CypherString(string_to_encrypt)
        auth = types.AuthorizationToken(
            "basic", principal="neo4j", credentials="pass"
        )
        driver = Driver(
            backend, "bolt://localhost:9999", auth,
            property_encryption_profiles=[
                {"name": INTEROP_PROFILE_NAME, "kek": kek}
            ],
        )
        try:
            key = driver.create_encapsulated_key(
                "k", profile_name=INTEROP_PROFILE_NAME
            )
            encrypted = driver.encrypt_to_bytes(
                value, profile_name=INTEROP_PROFILE_NAME, key_alias="k"
            )
        finally:
            driver.close()

        print_fixture_literal(
            driver=get_driver_name(),
            kek=kek,
            encapsulation=key.encapsulated_bytes,
            metadata=key.metadata,
            encrypted=encrypted,
            value=string_to_encrypt,
        )
    finally:
        backend.close()


def print_fixture_literal(*, driver, kek, encapsulation, metadata, encrypted,
                          value):
    metadata_literal = ", ".join(
        f'"{key}": "{val}"' for key, val in metadata.items()
    )
    print("    DecryptInteropFixture(")
    print(f'        driver="{driver}",')
    print_bytes_literal("kek", kek)
    print_bytes_literal("encapsulation", encapsulation)
    print(f"        metadata={{{metadata_literal}}},")
    print_bytes_literal("encrypted", encrypted)
    print(f'        value=types.CypherString("{value}"),')
    print("    ),")


def print_bytes_literal(field, value):
    print(f"        {field}=bytes.fromhex(")
    hex_string = value.hex()
    for i in range(0, len(hex_string), 32):
        print(f'            "{hex_string[i:i + 32]}"')
    print("        ),")


if __name__ == "__main__":
    main()
