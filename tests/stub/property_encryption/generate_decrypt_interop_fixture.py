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
"""

import secrets

import nutkit.protocol as types
from nutkit.frontend import Driver
from tests.shared import get_driver_name, new_backend
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
                {"name": INTEROP_PROFILE_NAME, "fixed_kek": kek}
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
            encapsulation=bytes.fromhex(key.encapsulated_bytes.value),
            metadata=key.metadata,
            encrypted=bytes.fromhex(encrypted.value),
            value=string_to_encrypt,
        )
    finally:
        backend.close()


def print_fixture_literal(*, driver, kek, encapsulation, metadata, encrypted,
                          value):
    print("DecryptInteropFixture(")
    print(f"    driver={driver!r},")
    print(f"    kek=bytes.fromhex({kek.hex()!r}),")
    print(f"    encapsulation=bytes.fromhex({encapsulation.hex()!r}),")
    print(f"    metadata={metadata!r},")
    print(f"    encrypted=bytes.fromhex({encrypted.hex()!r}),")
    print(f"    value=types.CypherString({value!r}),")
    print("),")


if __name__ == "__main__":
    main()
