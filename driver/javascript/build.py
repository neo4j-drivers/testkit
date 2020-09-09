"""
Executed in Javascript driver container.
Responsible for building driver and test backend.
"""
import os, subprocess, shutil


def run(args, env=None):
    subprocess.run(
        args, universal_newlines=True, stderr=subprocess.STDOUT, check=True, env=env)

if __name__ == "__main__":
    run(["npm", "install"])
    run(["gulp", "nodejs"])

