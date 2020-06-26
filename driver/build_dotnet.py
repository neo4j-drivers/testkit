"""
Executed in dotnet driver container.
Responsible for building driver and test backend.
"""
import os, subprocess, shutil


def run(args):
    subprocess.run(
        args, universal_newlines=True, stderr=subprocess.STDOUT)


if __name__ == "__main__":
    # Fetch all dependencies
    run([
        "dotnet", "restore", "--disable-parallel", "-v", "n", "DotNetNutkit/NutKitDotNet.csproj"
    ])
    run([
        "dotnet", "msbuild", "-v:n", "DotNetNutkit/NutKitDotNet.csproj"
    ])

