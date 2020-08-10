"""
Executed in Java driver container.
Assumes driver and backend has been built.
Responsible for starting the test backend.
"""
import os, subprocess


if __name__ == "__main__":
    subprocess.check_call(
        ["java", "-jar", "nutkit-backend/target/nutkit-backend-4.1-SNAPSHOT.jar"])

