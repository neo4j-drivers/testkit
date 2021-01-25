import subprocess
import os


FIVE_MINUTES = '300'

def run(args):
    subprocess.run(
        args, universal_newlines=True, stderr=subprocess.STDOUT, check=True)


if __name__ == "__main__":
    os.environ['STRESS_TEST_MODE'] = 'fastest'
    if os.environ.get('TEST_NEO4J_IS_CLUSTER'):
        os.environ['RUNNING_TIME_IN_SECONDS'] = FIVE_MINUTES
    run(["npm", "run", "run-stress-tests"])
