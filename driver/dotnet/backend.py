"""
Executed in dotnet driver container.
Assumes driver and backend has been built.
Responsible for starting the test backend.
"""
import os, subprocess

if __name__ == "__main__":
    backend_path = os.path.join(".", "Neo4j.Driver", "Neo4j.Driver.Tests.TestBackend", "bin", "Debug", "netcoreapp2.0", "Neo4j.Driver.Tests.TestBackend")
    logfile_path = os.path.join("..", "artifacts", "backend.log")
    subprocess.check_call([backend_path, "0.0.0.0", "9876", logfile_path])
