"""
Executed in Java driver container.
Assumes driver and backend has been built.
Responsible for starting the test backend.
"""
import os, subprocess
def run(args):
    subprocess.run(
        args, universal_newlines=True, stderr=subprocess.STDOUT)
if __name__ == "__main__":
    run(["java", "-jar", "nutkit-backend/target/nutkit-backend-4.1-SNAPSHOT.jar"])

