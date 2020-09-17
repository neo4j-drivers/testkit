"""
Executed in Go driver container.
Assumes driver and backend has been built.
Responsible for starting the test backend.
"""
import os, subprocess

if __name__ == "__main__":
    goPath = "/home/build"
    backendPath = os.path.join(goPath, "bin", "testkit-backend")
    err = open("/artifacts/backenderr.dump", "w")
    out = open("/artifacts/backendout.dump", "w")
    subprocess.check_call([backendPath], stdout=out, stderr=err)
