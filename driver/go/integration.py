import os, subprocess

root_package = "github.com/neo4j/neo4j-go-driver/neo4j"

def run(args):
    subprocess.run(
        args, universal_newlines=True, stderr=subprocess.STDOUT, check=True)

if __name__ == "__main__":
    os.environ["GOPATH"] = "/home/build"
    package = root_package + "/test-integration/..."
    run(["go", "get", "-v", "-t", package])
    cmd = ["go", "test"]
    if os.environ.get("TEST_IN_TEAMCITY", False):
        cmd = cmd + ["-v", "-json"]
    run(cmd + [package])

