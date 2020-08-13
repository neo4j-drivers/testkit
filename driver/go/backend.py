"""
Executed in Go driver container.
Assumes driver and backend has been built.
Responsible for starting the test backend.
"""
import os, subprocess

if __name__ == "__main__":
    goPath = "/home/build"
    backendPath = os.path.join(goPath, "bin", "testkit-backend")
    subprocess.check_call([backendPath])
