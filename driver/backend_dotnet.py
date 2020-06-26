"""
Executed in dotnet driver container.
Assumes driver and backend has been built.
Responsible for starting the test backend.
"""
import os, subprocess

if __name__ == "__main__":
    backend_path = os.path.join(".", "DotNetNutkit", "bin", "Debug", "netcoreapp3.1", "NutKitDotNet")
    subprocess.check_call([backend_path, "0.0.0.0", 9876])
