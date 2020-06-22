"""
Executed in Go driver container.
Responsible for building driver and test backend.
"""
import os, subprocess, shutil


if __name__ == "__main__":
    # Setup a Go path environment to build in (since we allow Go 1.10)
    # Assumes the current directory is Go driver repo root
    # Use build root as go path
    goPath = "/home/build"
    buildPath = os.path.join(goPath, "src", "github.com", "neo4j", "neo4j-go-driver")
    shutil.copytree(".", buildPath)
    os.environ["GOPATH"] = goPath
    subprocess.run([
        "go", "get", "github.com/neo4j/neo4j-go-driver/..."])
    subprocess.run([
        "go", "install","-v", "github.com/neo4j/neo4j-go-driver/nutbackend"])
