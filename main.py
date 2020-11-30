"""
Runs all test suites in their appropriate context.
The same test suite might be executed on different Neo4j server versions or
setups, it is the responsibility of this script to setup these contexts and
orchestrate which suites that are executed in each context.
"""

import os
import sys
import atexit
import subprocess
import shutil
from tests.testenv import (
        begin_test_suite, end_test_suite, in_teamcity)
import docker
import teamcity


containers = ["driver", "neo4jserver", "runner"]
networks = ["the-bridge"]


def cleanup():
    for c in containers:
        subprocess.run(["docker", "rm", "-f", "-v", c],
                       check=False, stdout=subprocess.DEVNULL,
                       stderr=subprocess.DEVNULL)
    for n in networks:
        subprocess.run(["docker", "network", "rm", n],
                       check=False, stdout=subprocess.DEVNULL,
                       stderr=subprocess.DEVNULL)


def ensure_driver_image(root_path, branch_name, driver_name):
    """ Ensures that an up to date Docker image exists for the driver.
    """
    # Construct Docker image name from driver name (i.e drivers-go) and
    # branch name (i.e 4.2, go-1.14-image)
    image_name = "drivers-%s:%s" % (driver_name, branch_name)
    # Context path is the Docker build context, all files added/copied to the
    # driver image needs to be here.
    context_path = os.path.join(root_path, "driver", driver_name)
    # Copy CAs that the driver should know of to the Docker build context
    # (first remove any previous...). Each driver container should contain
    # those CAs in such a way that driver language can use them as system
    # CAs without any custom modification of the driver.
    cas_path = os.path.join(context_path, "CAs")
    shutil.rmtree(cas_path, ignore_errors=True)
    cas_source_path = os.path.join(root_path, "tests", "tls",
                                   "certs", "driver")
    shutil.copytree(cas_source_path, cas_path)

    # This will use the driver folder as build context.
    print("Building driver Docker image %s from %s"
          % (image_name, context_path))
    subprocess.check_call([
        "docker", "build", "--tag", image_name, context_path])

    print("Checking for dangling intermediate images")
    images = subprocess.check_output([
        "docker", "images", "-a", "--filter=dangling=true", "-q"
    ], encoding="utf-8").splitlines()
    if len(images):
        print("Cleaning up images: %s" % images)
        # Sometimes fails, do not fail build due to that
        subprocess.run(["docker", "rmi", " ".join(images)])

    return image_name


def main(thisPath, driverName, testkitBranch, driverRepo):
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

    # Bootstrap the driver docker image by running a bootstrap script in
    # the image. The driver docker image only contains the tools needed to
    # build, not the built driver.
    driverContainer = docker.run(
            driverImage, "driver",
            command=["python3", "/testkit/driver/bootstrap.py"],
            mountMap={
                thisPath: "/testkit",
                driverRepo: "/driver",
                artifactsPath: "/artifacts"
            },
            portMap={9876: 9876},  # For convenience when debugging
            network="the-bridge",
            workingFolder="/driver")

    # Setup environment variables for driver container
    driverEnv = {}
    # Copy TEST_ variables that might have been set explicit
    for varName in os.environ:
        if varName.startswith("TEST_"):
            driverEnv[varName] = os.environ[varName]

    # Clean up artifacts
    driverContainer.exec(
            ["python3", "/testkit/driver/clean_artifacts.py"],
            envMap=driverEnv)

    # Build the driver and it's testkit backend
    print("Build driver and test backend in driver container")
    driverContainer.exec(
            ["python3", "/testkit/driver/%s/build.py" % driverName],
            envMap=driverEnv)
    print("Finished building driver and test backend")

    """
    Unit tests
    """
    begin_test_suite('Unit tests')
    driverContainer.exec(
            ["python3", "/testkit/driver/%s/unittests.py" % driverName],
            envMap=driverEnv)
    end_test_suite('Unit tests')

    # Start the test backend in the driver Docker instance.
    # Note that this is done detached which means that we don't know for
    # sure if the test backend actually started and we will not see
    # any output of this command.
    # When failing due to not being able to connect from client or seeing
    # issues like 'detected possible backend crash', make sure that this
    # works simply by commenting detach and see that the backend starts.
    print("Start test backend in driver container")
    driverContainer.exec_detached(
            ["python3", "/testkit/driver/%s/backend.py" % driverName],
            envMap=driverEnv)
    # Wait until backend started
    # Use driver container to check for backend availability
    driverContainer.exec([
        "python3",
        "/testkit/driver/wait_for_port.py", "localhost", "%d" % 9876],
        envMap=driverEnv)
    print("Started test backend")

    # Start runner container, responsible for running the unit tests.
    # Use Go driver image for this since we need to build TLS server and
    # use that in the runner.
    runnerImage = ensure_driver_image(thisPath, testkitBranch, "go")
    runnerEnv = {
        "PYTHONPATH": "/testkit",  # To use modules
    }
    # Copy TEST_ variables that might have been set explicit
    for varName in os.environ:
        if varName.startswith("TEST_"):
            runnerEnv[varName] = os.environ[varName]
    # Override with settings that must have a known value
    username = "neo4j"
    password = "pass"
    neo4jServerHostname = "neo4jserver"
    port = 7687
    runnerEnv.update({
        # Hostname of Docker container runnng db
        "TEST_NEO4J_HOST":   neo4jServerHostname,
        "TEST_NEO4J_USER":   username,
        "TEST_NEO4J_PASS":   password,
        # Runner connects to backend in driver container
        "TEST_BACKEND_HOST": "driver",
        # Driver connects to me
        "TEST_STUB_HOST":    "runner",
    })
    runnerContainer = docker.run(
            runnerImage, "runner",
            command=["python3", "/testkit/driver/bootstrap.py"],
            mountMap={thisPath: "/testkit"},
            envMap=runnerEnv,
            network="the-bridge",
            aliases=["thehost", "thehostbutwrong"])  # Used when testing TLS

    """
    Stub tests
    """
    runnerContainer.exec(["python3", "-m", "tests.stub.suites"])

    """
    TLS tests
    """
    # Build TLS server
    runnerContainer.exec(
            ["go", "build", "-v", "."], workdir="/testkit/tlsserver")
    runnerContainer.exec(
            ["python3", "-m", "tests.tls.suites"])

    """
    Neo4j server tests
    """
    # Make an artifacts folder where the database can place it's logs, each
    # time we start a database server we should use a different folder.
    neo4jArtifactsPath = os.path.join(artifactsPath, "neo4j")
    os.makedirs(neo4jArtifactsPath)

    neo4jServers = [
        {
            "image": "neo4j:3.5-enterprise",
            "version": "3.5",
            "edition": "enterprise",
            "cluster": False,
            "suite": "3.5",
            "scheme": "bolt"
        },
        {
            "image": "neo4j:4.0",
            "version": "4.0",
            "edition": "community",
            "cluster": False,
            "suite": "4.0",
            "scheme": "neo4j"
        },
        {
            "image": "neo4j:4.1-enterprise",
            "version": "4.1",
            "edition": "enterprise",
            "cluster": False,
            "suite": "4.1",
            "scheme": "neo4j"
        },
    ]
    if in_teamcity:
        # Use last successful build of 4.2.0. Need to update this when a new
        # patch is in the baking.  When there is an official 4.2 build there
        # should be a Docker hub based image above (or added when not in
        # Teamcity).
        s = {
            "image": "neo4j:4.2.2-enterprise",
            "version": "4.2",
            "edition": "enterprise",
            "cluster": False,
            "suite": "4.2",
            "scheme": "neo4j",
            "download": teamcity.DockerImage(
                "neo4j-enterprise-4.2.2-docker-loadable.tar")
        }
        neo4jServers.append(s)

    for neo4jServer in neo4jServers:
        # Environment variables passed to the Neo4j docker container
        envMap = {
            "NEO4J_dbms_connector_bolt_advertised__address": "%s:%d" % (
                neo4jServerHostname, port),
            "NEO4J_AUTH": "%s/%s" % (username, password),
        }
        if neo4jServer["edition"] != "community":
            envMap["NEO4J_ACCEPT_LICENSE_AGREEMENT"] = "yes"

        download = neo4jServer.get('download', None)
        if download:
            print("Downloading Neo4j docker image")
            docker.load(download.get())

        # Start a Neo4j server
        print("Starting neo4j server")
        docker.run(
                neo4jServer["image"], neo4jServerHostname,
                mountMap={os.path.join(neo4jArtifactsPath, "logs"): "/logs"},
                envMap=envMap,
                network="the-bridge")
        print("Neo4j container server started, waiting for port to "
              "be available")

        # Wait until server is listening before running tests
        # Use driver container to check for Neo4j availability since connect
        # will be done from there
        driverContainer.exec([
            "python3", "/testkit/driver/wait_for_port.py",
            neo4jServerHostname, "%d" % port])
        print("Neo4j in container listens")

        # Run the actual test suite within the runner container. The tests
        # will connect to driver backend and configure drivers to connect to
        # the neo4j instance.
        runnerContainer.exec([
            "python3", "-m", "tests.neo4j.suites", neo4jServer["suite"]])

        # Parameters that might be used by native stress/integration
        # tests suites
        driverEnv.update({
            "TEST_NEO4J_HOST":       neo4jServerHostname,
            "TEST_NEO4J_USER":       username,
            "TEST_NEO4J_PASS":       password,
            "TEST_NEO4J_SCHEME":     neo4jServer["scheme"],
            "TEST_NEO4J_PORT":       port,
            "TEST_NEO4J_EDITION":    neo4jServer["edition"],
            "TEST_NEO4J_VERSION":    neo4jServer["version"],
            "TEST_NEO4J_IS_CLUSTER": neo4jServer["cluster"],
        })

        # To support the legacy .net integration tests
        envString = ""
        if neo4jServer["edition"] == "enterprise":
            envString += "-e "
        envString += neo4jServer["version"]
        driverEnv["NEOCTRL_ARGS"] = envString

        # Run the stress test suite within the driver container.
        # The stress test suite uses threading and put a bigger load on the
        # driver than the integration tests do and are therefore written in
        # the driver language.
        print("Building and running stress tests...")
        driverContainer.exec([
            "python3", "/testkit/driver/%s/stress.py" % driverName],
            envMap=driverEnv)

        # Run driver native integration tests within the driver container.
        driverContainer.exec([
            "python3", "/testkit/driver/%s/integration.py" % driverName],
            envMap=driverEnv)

        # Check that all connections to Neo4j has been closed.
        # Each test suite should close drivers, sessions properly so any
        # pending connections detected here should indicate connection leakage
        # in the driver.
        print("Checking that connections are closed to the database")
        driverContainer.exec([
            "python3", "/testkit/driver/assert_conns_closed.py",
            neo4jServerHostname, "%d" % port])

        subprocess.run([
            "docker", "rm", "-f", "-v", neo4jServerHostname], check=False)


if __name__ == "__main__":
    driverName = os.environ.get("TEST_DRIVER_NAME")
    if not driverName:
        print("Missing environment variable TEST_DRIVER_NAME that contains "
              "name of the driver")
        sys.exit(1)

    testkitBranch = os.environ.get("TEST_BRANCH")
    if not testkitBranch:
        if in_teamcity:
            print("Missing environment variable TEST_BRANCH that contains "
                  "name of testkit branch. "
                  "This name is used to name Docker repository. ")
            sys.exit(1)
        testkitBranch = "local"

    # Retrieve path to the repository containing this script.
    # Use this path as base for locating a whole bunch of other stuff.
    # Add this path to python sys path to be able to invoke modules
    # from this repo
    thisPath = os.path.dirname(os.path.abspath(__file__))
    os.environ['PYTHONPATH'] = thisPath

    # Retrieve path to driver git repository
    driverRepo = os.environ.get("TEST_DRIVER_REPO")
    if not driverRepo:
        print("Missing environment variable TEST_DRIVER_REPO that contains "
              "path to driver repository")
        sys.exit(1)
    main(thisPath, driverName, testkitBranch, driverRepo)
