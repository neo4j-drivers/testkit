"""
Executed in Go driver container.
Responsible for running unit tests.
Assumes driver has been setup by build script prior to this.
"""
import os, subprocess

root_package = "github.com/neo4j/neo4j-go-driver/neo4j"

def run(args):
    subprocess.run(
        args, universal_newlines=True, stderr=subprocess.STDOUT)

if __name__ == "__main__":
    os.environ["GOPATH"] = "/home/build"
    # Install test dependencies
    run([
        "go", "get", "-t", root_package])
    # Run explicit set of unit tests to avoid running integration and stub tests
    run([
        "go", "test", "-v", "-json", root_package, "--race"])
