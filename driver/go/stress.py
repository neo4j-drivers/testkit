"""
Executed in Go driver container.
Responsible for running stress tests.
Assumes driver has been setup by build script prior to this.
"""
import os, subprocess, sys

root_package = "github.com/neo4j/neo4j-go-driver"

def run(args):
    subprocess.run(
        args, universal_newlines=True, stderr=subprocess.STDOUT, check=True)

if __name__ == "__main__":
    uri = "%s://%s:%s" % (os.environ["TEST_NEO4J_SCHEME"], os.environ["TEST_NEO4J_HOST"], os.environ["TEST_NEO4J_PORT"])
    user = os.environ["TEST_NEO4J_USER"]
    password = os.environ["TEST_NEO4J_PASS"]

    # Build the test-stress application
    os.environ["GOPATH"] = "/home/build"
    run(["go", "install", "-v", "--race", root_package + "/test-stress"])
    # Run the stress tests
    run(["/home/build/bin/test-stress", "-uri", uri, "-user", user, "-password", password])
