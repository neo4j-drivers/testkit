import subprocess
import os


def run(args):
    subprocess.run(
        args, universal_newlines=True, stderr=subprocess.STDOUT, check=True)


if __name__ == "__main__":
    os.environ['STRESS_TEST_MODE'] = 'fastest'
    run(["npm", "run", "run-stress-tests"])
