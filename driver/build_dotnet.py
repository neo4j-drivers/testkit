"""
Executed in dotnet driver container.
Responsible for building driver and test backend.
"""
import os, subprocess, shutil


def run(args):
    subprocess.run(
        args, universal_newlines=True, stderr=subprocess.STDOUT, check=True)


if __name__ == "__main__":
    # Fetch all dependencies
    run([
        "dotnet", "restore", "--disable-parallel", "-v", "n", "Neo4j.Driver/Neo4j.Driver.sln"
    ])
    run([
        "dotnet", "msbuild", "Neo4j.Driver/Neo4j.Driver.sln"
    ])

