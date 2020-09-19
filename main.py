"""
Runs all test suites in their appropriate context.
The same test suite might be executed on different Neo4j server versions or setups, it is the
responsibility of this script to setup these contexts and orchestrate which suites that
is executed in each context.
"""

import os, sys, atexit, subprocess, time, unittest, json, shutil
from datetime import datetime
from testenv import get_test_result_class, begin_test_suite, end_test_suite, in_teamcity
import docker

import tests.neo4j.suites as suites
#from tests.stub.suites import stub_suite
#from tests.tls.suites import tls_suite


# Environment variables
env_driver_name    = 'TEST_DRIVER_NAME'
env_driver_repo    = 'TEST_DRIVER_REPO'
env_testkit_branch = 'TEST_BRANCH'

containers = ["driver", "neo4jserver", "gobuilder"]
networks = ["the-bridge"]


def cleanup():
    for c in containers:
        subprocess.run(["docker", "rm", "-f", "-v", c],
            check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    for n in networks:
        subprocess.run(["docker", "network", "rm", n],
            check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def ensure_driver_image(root_path, branch_name, driver_name):
    """ Ensures that an up to date Docker image exists for the driver.
    """
    # Construct Docker image name from driver name (i.e drivers-go, drivers-java)
    # and branch name (i.e 4.2, go-1.14-image)
    image_name = "drivers-%s:%s" % (driver_name, branch_name)
    # Context path is the Docker build context, all files added/copied to the
    # driver image needs to be here.
    context_path = os.path.join(root_path, "driver", driver_name)
    # Copy CAs that the driver should know of to the Docker build context
    # (first remove any previous...). Each driver container should contain those CAs
    # in such a way that driver language can use them as system CAs without any
    # custom modification of the driver.
    cas_path = os.path.join(context_path, "CAs")
    shutil.rmtree(cas_path, ignore_errors=True)
    shutil.copytree(os.path.join(root_path, "tests", "tls", "certs", "driver"), cas_path)

    # Build the image using caches, this will rebuild the image if it has changed
    # in git repo or if the image doesn't exist on this agent/host. A build will
    # occure once per driver/agent/branch (reusing layers for different branches if
    # the image file hasn't changed).
    #
    # This will use the driver folder as build context.
    print("Building driver Docker image %s from %s" % (image_name, context_path))
    subprocess.check_call(["docker", "build", "--tag", image_name, context_path])

    print("Checking for dangling intermediate images")
    images = subprocess.check_output([
        "docker", "images", "-a", "--filter=dangling=true", "-q"
    ], encoding="utf-8").splitlines()
    if len(images):
        print("Cleaning up images: %s" % images)
        subprocess.check_call(["docker", "rmi", " ".join(images)])

    return image_name


if __name__ == "__main__":
    driverName = os.environ.get(env_driver_name)
    if not driverName:
        print("Missing environment variable %s that contains name of the driver" % env_driver_name)
        sys.exit(1)

    testkitBranch = os.environ.get(env_testkit_branch)
    if not testkitBranch:
        if in_teamcity:
            print("Missing environment variable %s that contains name of testkit branch. "
                  "This name is used to name Docker repository. " % env_testkit_branch)
            sys.exit(1)
        testkitBranch = "local"

    # Retrieve path to the repository containing this script.
    # Use this path as base for locating a whole bunch of other stuff.
    # Add this path to python sys path to be able to invoke modules from this repo
    thisPath = os.path.dirname(os.path.abspath(__file__))
    os.environ['PYTHONPATH'] = thisPath

    # Retrieve path to driver git repository
    driverRepo = os.environ.get(env_driver_repo)
    if not driverRepo:
        print("Missing environment variable %s that contains path to driver repository" % env_driver_repo)
        sys.exit(1)

    driverImage = ensure_driver_image(thisPath, testkitBranch, driverName)
    if not driverImage:
        sys.exit(1)

    # Prepare collecting of artifacts, collected to ./artifcats/
    artifactsPath = os.path.abspath(os.path.join(".", "artifacts"))
    if not os.path.exists(artifactsPath):
        os.makedirs(artifactsPath)
    print("Putting artifacts in %s" % artifactsPath)


    # Important to stop all docker images upon exit
    # Also make sure that none of those images are running at this point
    atexit.register(cleanup)
    cleanup()

    # Create network to be shared among the instances.
    # The host running this will be gateway on that network, retrieve that
    # address to be able to start services on the network that the driver
    # connects to (stub server and TLS server).
    subprocess.run([
        "docker", "network", "create", "the-bridge"
    ])

    # Bootstrap the driver docker image by running a bootstrap script in the image.
    # The driver docker image only contains the tools needed to build, not the built driver.
    tlsserverAddress = "127.0.0.1"
    driverContainer = docker.run(driverImage, "driver",
        command=["python3", "/nutkit/driver/bootstrap.py"],
        mountMap={thisPath: "/nutkit", driverRepo: "/driver", artifactsPath: "/artifacts"},
        portMap={9876: 9876},
        hostMap={"thehost": tlsserverAddress, "thehostbutwrong": tlsserverAddress},
        envMap={"PYTHONPATH": "/nutkit", env_driver_name: driverName},
        network="the-bridge",
        workingFolder="/driver")

    # Clean up artifacts
    driverContainer.exec(["python3", "/nutkit/driver/clean_artifacts.py"])

    # Build the driver and it's nutkit backend
    print("Build driver and test backend in driver container")
    driverContainer.exec(["python3", "/nutkit/driver/%s/build.py" % driverName])
    print("Finished building driver and test backend")

    """
    Unit tests
    """
    begin_test_suite('Unit tests')
    driverContainer.exec(["python3", "/nutkit/driver/%s/unittests.py" % driverName])
    end_test_suite('Unit tests')

    # Start the test backend in the driver Docker instance.
    # Note that this is done detached which means that we don't know for
    # sure if the test backend actually started and we will not see
    # any output of this command.
    # When failing due to not being able to connect from client or seeing
    # issues like 'detected possible backend crash', make sure that this
    # works simply by commenting detach and see that the backend starts.
    print("Start test backend in driver container")
    driverContainer.exec_detached(["python3", "/nutkit/driver/%s/backend.py" % driverName])
    # Wait until backend started
    # Use driver container to check for backend availability
    driverContainer.exec(["python3", "/nutkit/driver/wait_for_port.py", "localhost", "%d" % 9876])
    print("Started test backend")

    failed = False

    """
    Stub tests
    """
    driverContainer.exec(["python3", "-m", "stubrunner"])

    """
    Neo4j 4.0 server tests
    """
    # TODO: Write suite name to TeamCity
    # Make an artifacts folder where the database can place it's logs, each time
    # we start a database server we should use a different folder.
    neo4jArtifactsPath = os.path.join(artifactsPath, "neo4j")
    os.makedirs(neo4jArtifactsPath)
    neo4jserver = "neo4jserver"

    neo4jservers = [
        { "image_name": "neo4j:4.0", "suite": suites.single_community_neo4j4x0 },
        { "image_name": "neo4j:4.1", "suite": suites.single_enterprise_neo4j4x1 },
    ]
    for ix in neo4jservers:
        # Start a Neo4j server
        print("Starting neo4j server")
        neo4jContainer = docker.run(ix["image_name"], neo4jserver,
            mountMap={os.path.join(neo4jArtifactsPath, "logs"): "/logs"},
            envMap={"NEO4J_dbms_connector_bolt_advertised__address": "%s:7687" % neo4jserver, "NEO4J_AUTH": "%s/%s" % ("neo4j", "pass")},
            network="the-bridge")
        print("Neo4j container server started, waiting for port to be available")

        # Wait until server is listening before running tests
        # Use driver container to check for Neo4j availability since connect will be done from there
        driverContainer.exec(["python3", "/nutkit/driver/wait_for_port.py", neo4jserver, "%d" % 7687])
        print("Neo4j in container listens")

        # Make sure that the tests instruct the driver to connect to neo4jserver docker container
        os.environ['TEST_NEO4J_HOST'] = neo4jserver

        suiteName = "Integration tests, %s" % ix["image_name"]
        begin_test_suite(suiteName)
        runner = unittest.TextTestRunner(resultclass=get_test_result_class(), verbosity=100)
        result = runner.run(ix["suite"])
        if result.errors or result.failures:
            failed = True
        end_test_suite(suiteName)
        if failed:
            sys.exit(1)

        # Check that all connections to Neo4j has been closed.
        # Each test suite should close drivers, sessions properly so any pending connections
        # detected here should indicate connection leakage in the driver.
        driverContainer.exec(["python3", "/nutkit/driver/assert_conns_closed.py", neo4jserver, "%d" % 7687])

        subprocess.run([
            "docker", "stop", "neo4jserver"])

    """
    TLS tests
    """
    print("Building TLS server in Go image to be used for TLS tests")
    goBuilderImage = ensure_driver_image(thisPath, testkitBranch, "go")
    docker.run(goBuilderImage, "gobuilder",
        mountMap={thisPath: "/testkit"},
        workingFolder="/testkit/tlsserver",
        command=["go", "build", "-v", "."])
    print("Finished building TLS server, ready for use")
    driverContainer.exec(["python3", "-m", "tlsrunner"])

