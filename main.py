"""
Runs all test suites in their appropriate context.
The same test suite might be executed on different Neo4j server versions or setups, it is the
responsibility of this script to setup these contexts and orchestrate which suites that
is executed in each context.
"""

import os, sys, atexit, subprocess, time, unittest

import tests.neo4j.suites as suites
import tests.stub.suites as stub_suites


# Environment variables
env_driver_repo  = 'TEST_DRIVER_REPO'
env_driver_image = 'TEST_DRIVER_IMAGE'
env_teamcity     = 'TEST_IN_TEAMCITY'


containers = ["driver", "neo4jserver"]
networks = ["the-bridge"]


def cleanup():
    for c in containers:
        subprocess.run(["docker", "rm", "-f", "-v", c],
            check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    for n in networks:
        subprocess.run(["docker", "network", "rm", n],
            check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


class TeamCityTestResult(unittest.TextTestResult):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def startTest(self, test):
        print("##teamcity[testStarted name='%s']" % test)

    def stopTest(self, test):
        print("##teamcity[testFinished name='%s']" % test)

    def addError(self, test, err):
        print("##teamcity[testFailed name='%s' message='%s' details='%s']" % (test, err[1], err[2]))

    def addFailure(self, test, err):
        print("##teamcity[testFailed name='%s' message='%s' details='%s']" % (test, err[1], err[2]))

    def addSkip(self, test, reason):
        print("##teamcity[testIgnored name='%s' message='%s']" % (test, reason))

def get_test_result_class():
    in_teamcity = os.environ.get(env_teamcity)
    if not in_teamcity:
        return None
    return TeamCityTestResult

if __name__ == "__main__":
    # Retrieve path to driver git repository
    driverRepo = os.environ.get(env_driver_repo)
    if not driverRepo:
        print("Missing environment variable %s that contains path to driver repository" % env_driver_repo)
        sys.exit(1)

    # Retrieve name of driver Docker image.
    # The driver Docker image should be capable of building the driver and the test backend.
    # Python3.6 or later must be installed on the image.
    driverImage = os.environ.get(env_driver_image)
    if not driverImage:
        print("Missing environment variable %s that contains name of driver Docker image" % env_driver_image)
        sys.exit(1)


    # Retrieve path to the repository containing this script, assumes we're in the root of the
    # repository.
    nutRepo = os.path.dirname(os.path.abspath(__file__))

    # Important to stop all docker images upon exit
    atexit.register(cleanup)

    # Also make sure that none of those images are running at this point
    cleanup()

    # Create network to be shared among the instances
    subprocess.run([
        "docker", "network", "create", "the-bridge"
    ])

    # Bootstrap the driver docker image by running a bootstrap script in the image.
    # The driver docker image only contains the tools needed to build, not the built driver.
    p = subprocess.Popen([
        "docker", "run",
        # Bootstrap script is in the repo containing this script mounted as /nutkit
        "-v", "%s:/nutkit" % nutRepo,
        # The driver repo mounted as /driver
        "-v", "%s:/driver" % driverRepo,
        # Name of the docker container
        "--name", "driver",
        # Expose backend on this port
        "-p9876:9876",
        # Set working folder to the driver
        "-w", "/driver",
        # Remove itself upon exit
        "--rm",
        "--net=the-bridge",
        # Name of the image
        driverImage,
        # Bootstrap command
        "python3", "/nutkit/driver/bootstrap.py"
    ], bufsize=0, encoding='utf-8', stdout=subprocess.PIPE)
    print("Waiting for driver container to start")
    line = p.stdout.readline()
    if line.strip() != "ok":
        print(line)
        sys.exit(2)
    print("Started")


    # Build the driver and it's nutkit backend
    print("Build nutkit backend in driver container")
    subprocess.run([
        "docker", "exec",
        "driver",
        "python3", "/nutkit/driver/build_go.py"
    ])
    print("Finished building")


    """
    TODO:
    Unit tests
    """



    print("Start backend in driver container")
    subprocess.run([
        "docker", "exec",
        "--detach",
        "driver",
        "python3", "/nutkit/driver/backend_go.py"
    ])
    print("Started")

    failed = False

    """
    TODO:
    Stub tests, protocol version 4
    """
    print("Running stub suites")
    runner = unittest.TextTestRunner(resultclass=get_test_result_class())
    result = runner.run(stub_suites.protocol4x0)
    if result.errors or result.failures:
        failed = True

    if failed:
        sys.exit(1)


    """
    Neo4j 4.0 server tests
    """
    # TODO: Write suite name to TeamCity

    # Start a Neo4j server
    neo4jserver = "neo4jserver"
    print("Starting neo4j server")
    subprocess.run([
        "docker", "run",
        # Name of the docker container
        "--name", neo4jserver,
        # Remove itself upon exit
        "--rm",
        # Run in background
        "--detach",
        "--env", "NEO4J_dbms_connector_bolt_advertised__address=%s:7687" % neo4jserver,
        "--net=the-bridge",
        # Force a password
        "--env", "NEO4J_AUTH=%s/%s" % ("neo4j", "pass"),
        # Image
        "neo4j:latest",
    ])

    # Wait until server is listening before running tests
    time.sleep(10)
    print("Neo4j server started")

    # Make sure that the tests instruct the driver to connect to neo4jserver docker container
    os.environ['TEST_NEO4J_HOST'] = neo4jserver

    print("Running tests on server...")
    runner = unittest.TextTestRunner(resultclass=get_test_result_class())
    result = runner.run(suites.single_community_neo4j4x0)
    if result.errors or result.failures:
        failed = True

    if failed:
        sys.exit(1)


    """
    TODO:
    Neo4j 4.1 server tests
    """



