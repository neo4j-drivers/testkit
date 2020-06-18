import os, subprocess, shutil


if __name__ == "__main__":
    buildRoot = os.environ.get('BUILD_ROOT')
    if not buildRoot:
        print("Missing BUILD_ROOT")
        exit(1)
    # Setup a Go path environment to build in (since we allow Go 1.10)
    # Assumes the current directory is Go driver repo root
    # Use build root as go path
    goPath = buildRoot
    buildPath = os.path.join(goPath, "src", "github.com", "neo4j", "neo4j-go-driver")
    shutil.copytree(".", buildPath)
    os.environ["GOPATH"] = goPath
    subprocess.run([
        "go", "get", "github.com/neo4j/neo4j-go-driver/..."])
    subprocess.run([
        "go", "install","-v", "github.com/neo4j/neo4j-go-driver/nutbackend"])

    # Nutkit backend should be in build root
    shutil.copy2(os.path.join(goPath, "bin", "nutbackend"), os.path.join(goPath, "nutbackend"))
