"""
Executed in dotnet driver container.
Assumes driver and backend has been built.
Responsible for starting the test backend.
"""
import os, subprocess

if __name__ == "__main__":
    backend_path = os.path.join(".", "Neo4j.Driver.Tests.TestBackend", "bin", "Debug", "netcoreapp3.1", "Neo4j.Driver.Tests.TestBackend"]
    subprocess.check_call([backend_path, "0.0.0.0", 9876])
