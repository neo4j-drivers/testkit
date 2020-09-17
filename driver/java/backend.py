"""
Executed in Java driver container.
Assumes driver and backend has been built.
Responsible for starting the test backend.
"""
import os, subprocess


if __name__ == "__main__":
    err = open("/artifacts/backenderr.dump", "w")
    out = open("/artifacts/backendout.dump", "w")
    subprocess.check_call(
        ["java", "-jar", "testkit-backend/target/testkit-backend-4.1-SNAPSHOT.jar"], stdout=out, stderr=err)

