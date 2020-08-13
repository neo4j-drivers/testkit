"""
Executed in Go driver container.
Responsible for building driver and test backend.
"""
import os, subprocess, shutil


def run(args):
    subprocess.run(
        args, universal_newlines=True, stderr=subprocess.STDOUT, check=True)

if __name__ == "__main__":
    # Setup a Go path environment to build in (since we allow Go 1.10)
    # Assumes the current directory is Go driver repo root
    # Use build root as go path
    goPath = "/home/build"
    buildPath = os.path.join(goPath, "src", "github.com", "neo4j", "neo4j-go-driver")
    shutil.copytree(".", buildPath)
    os.environ["GOPATH"] = goPath
    run(["go", "get", "-v", "github.com/neo4j/neo4j-go-driver/..."])
    run(["go", "install","-v", "github.com/neo4j/neo4j-go-driver/testkit-backend"])
