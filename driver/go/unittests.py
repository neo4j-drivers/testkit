"""
Executed in Go driver container.
Responsible for running unit tests.
Assumes driver has been setup by build script prior to this.
"""
import os, subprocess

root_package = "github.com/neo4j/neo4j-go-driver/neo4j"

def run(args):
    subprocess.run(
        args, universal_newlines=True, stderr=subprocess.STDOUT, check=True)

if __name__ == "__main__":
    os.environ["GOPATH"] = "/home/build"
    # Install test dependencies
    run([
        "go", "get", "-t", root_package])
    # Run explicit set of unit tests to avoid running integration and stub tests
    # Specify -v -json to make TeamCity pickup the tests
    # When check is True when we will fail fast and probable not continue
    # running any other test suites either (like integration and stubs)
    run([
        "go", "test", "-v", "-json", root_package, "--race"])
    run([
        "go", "test", "-v", "-json", root_package + "/internal/...", "--race"])
