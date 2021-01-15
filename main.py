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
import neo4j
import argparse

networks = ["the-bridge"]

test_flags = {
    "TESTKIT_TESTS": True,
    "UNIT_TESTS": True,
    "INTEGRATION_TESTS": True,
    "STUB_TESTS": True,
    "STRESS_TESTS": True,
    "TLS_TESTS": True
}

configurations_to_run = []
configurations = []


def initialise_configurations():
    configurations.append({"name": "4.2-cluster",
                           "image": "neo4j:4.2-enterprise",
                           "version": "4.2",
                           "edition": "enterprise",
                           "cluster": True,
                           "suite": "",  # TODO: Define cluster suite
                           "scheme": "neo4j"})

    configurations.append({"name": "3.5-enterprise",
                           "image": "neo4j:3.5-enterprise",
                           "version": "3.5",
                           "edition": "enterprise",
                           "cluster": False,
                           "suite": "3.5",
                           "scheme": "bolt"})

    configurations.append({"name": "4.0-community",
                           "version": "4.0",
                           "image": "neo4j:4.0",
                           "edition": "community",
                           "cluster": False,
                           "suite": "4.0",
                           "scheme": "neo4j"})

    configurations.append({"name": "4.1-enterprise",
                           "image": "neo4j:4.1-enterprise",
                           "version": "4.1",
                           "edition": "enterprise",
                           "cluster": False,
                           "suite": "4.1",
                           "scheme": "neo4j"})

    if in_teamcity:
        configurations.append({"name": "4.2-tc-enterprise",
                               "image": "neo4j:4.2.3-enterprise",
                               "version": "4.2",
                               "edition": "enterprise",
                               "cluster": False,
                               "suite": "4.2",
                               "scheme": "neo4j",
                               "download": teamcity.DockerImage("neo4j-enterprise-4.2.3-docker-loadable.tar")})

        configurations.append({"name": "4.3-tc-enterprise",
                               "image": "neo4j:4.3.0-drop02.0-enterprise",
                               "version": "4.3",
                               "edition": "enterprise",
                               "cluster": False,
                               "suite": "4.3",
                               "scheme": "neo4j",
                               "download": teamcity.DockerImage("neo4j-enterprise-4.3.0-drop02.0-docker-loadable.tar")})


def set_test_flags(requested_list):
    source = []
    if not requested_list:
        requested_list = test_flags

    for item in test_flags:
        if item not in requested_list:
            test_flags[item] = False

    print("Tests that will be run:")
    for item in test_flags:
        if test_flags[item]:
            print("     ", item)


def convert_to_str(input_seq, separator):
    # Join all the strings in list
    final_str = separator.join(input_seq)
    return final_str


def construct_configuration_list(requested_list):
    # if no configs were requested we will default to adding them all
    if not requested_list:
        requested_list = []
        for config in configurations:
            requested_list.append(config["name"])

    # Now try to find the requested configs and check they are available with current teamcity status
    for config in configurations:
        if config["name"] in requested_list:
            configurations_to_run.append(config)

    print("Accepted configurations:")
    for item in configurations_to_run:
        print("     ", item["name"])


def parse_command_line(argv):
    # setup the configurations that are available
    initialise_configurations()

    # create parser
    parser = argparse.ArgumentParser()

    test_help_string = "Optional space separated list selected from: " + convert_to_str(test_flags.keys(), ",  ")
    server_help_string = "Optional space separated list selected from: "
    for config in configurations:
        server_help_string += config["name"] + ", "

    # add arguments
    parser.add_argument("--tests", nargs='*', required=False, help=test_help_string)
    parser.add_argument("--configs", nargs='*', required=False, help=server_help_string)

    # parse the arguments
    args = parser.parse_args()

    set_test_flags(args.tests)
    construct_configuration_list(args.configs)


def cleanup():
    docker.cleanup()
    for n in networks:
        subprocess.run(["docker", "network", "rm", n],
                       check=False, stdout=subprocess.DEVNULL,
                       stderr=subprocess.DEVNULL)


def ensure_driver_image(root_path, driver_glue_path, branch_name, driver_name):
    """ Ensures that an up to date Docker image exists for the driver.
    """
    # Construct Docker image name from driver name (i.e drivers-go) and
    # branch name (i.e 4.2, go-1.14-image)
    image_name = "drivers-%s:%s" % (driver_name, branch_name)
    # Copy CAs that the driver should know of to the Docker build context
    # (first remove any previous...). Each driver container should contain
    # those CAs in such a way that driver language can use them as system
    # CAs without any custom modification of the driver.
    cas_path = os.path.join(driver_glue_path, "CAs")
    shutil.rmtree(cas_path, ignore_errors=True)
    cas_source_path = os.path.join(root_path, "tests", "tls",
                                   "certs", "driver")
    shutil.copytree(cas_source_path, cas_path)

    # This will use the driver folder as build context.
    print("Building driver Docker image %s from %s"
          % (image_name, driver_glue_path))
    subprocess.check_call([
        "docker", "build", "--tag", image_name, driver_glue_path])

    print("Checking for dangling intermediate images")
    images = subprocess.check_output([
        "docker", "images", "-a", "--filter=dangling=true", "-q"
    ], encoding="utf-8").splitlines()
    if len(images):
        print("Cleaning up images: %s" % images)
        # Sometimes fails, do not fail build due to that
        subprocess.run(["docker", "rmi", " ".join(images)])

    return image_name


def ensure_runner_image(root_path, testkitBranch):
    # Use Go driver image for this since we need to build TLS server and
    # use that in the runner.
    # TODO: Use a dedicated Docker image for this.
    return ensure_driver_image(
            root_path, os.path.join(root_path, "driver", "go"),
            testkitBranch, "go")


def get_driver_glue(thisPath, driverName, driverRepo):
    """ Locates where driver has it's docker image and Python "glue" scripts
    needed to build and run tests for the driver.
    Returns a tuple consisting of the absolute path on this machine along with
    the path as it will be mounted in the driver container.
    """
    in_driver_repo = os.path.join(driverRepo, "testkit")
    if os.path.isdir(in_driver_repo):
        return (in_driver_repo, "/driver/testkit")

    in_this_repo = os.path.join(thisPath, "driver", driverName)
    if os.path.isdir(in_this_repo):
        return (in_this_repo, "/testkit/driver/%s" % driverName)

    raise Exception("No glue found for %s" % driverName)


def main(thisPath, driverName, testkitBranch, driverRepo):
    # Path where scripts are that adapts driver to testkit.
    # Both absolute path and path relative to driver container.
    absGlue, driverGlue = get_driver_glue(
            thisPath, driverName, driverRepo)

    driverImage = ensure_driver_image(thisPath, absGlue,
                                      testkitBranch, driverName)

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
            ["python3", os.path.join(driverGlue, "build.py")],
            envMap=driverEnv)
    print("Finished building driver and test backend")

    """
    Unit tests
    """
    if test_flags["UNIT_TESTS"]:
        begin_test_suite('Unit tests')
        driverContainer.exec(
                ["python3", os.path.join(driverGlue, "unittests.py")],
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
            ["python3", os.path.join(driverGlue, "backend.py")],
            envMap=driverEnv)
    # Wait until backend started
    # Use driver container to check for backend availability
    driverContainer.exec([
        "python3",
        "/testkit/driver/wait_for_port.py", "localhost", "%d" % 9876],
        envMap=driverEnv)
    print("Started test backend")

    # Start runner container, responsible for running the unit tests.
    runnerImage = ensure_runner_image(thisPath, testkitBranch)
    runnerEnv = {
        "PYTHONPATH": "/testkit",  # To use modules
    }
    # Copy TEST_ variables that might have been set explicit
    for varName in os.environ:
        if varName.startswith("TEST_"):
            runnerEnv[varName] = os.environ[varName]
    # Override with settings that must have a known value
    runnerEnv.update({
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
    if test_flags["STUB_TESTS"]:
        runnerContainer.exec(["python3", "-m", "tests.stub.suites"])

    """
    TLS tests
    """
    # Build TLS server
    if test_flags["TLS_TESTS"]:
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

    # Until expanded protocol is implemented then we will only support current
    # version + previous 2 minors + last minor version of last major version.

    for neo4j_config in configurations_to_run:
        download = neo4j_config.get('download', None)
        if download:
            print("Downloading Neo4j docker image")
            docker.load(download.get())

        cluster = neo4j_config["cluster"]
        serverName = neo4j_config["name"]

        # Start a Neo4j server
        if cluster:
            print("Starting neo4j cluster (%s)" % serverName)
            server = neo4j.Cluster(neo4j_config["image"],
                                   serverName,
                                   neo4jArtifactsPath)
        else:
            print("Starting neo4j standalone server (%s)" % serverName)
            server = neo4j.Standalone(neo4j_config["image"],
                                      serverName,
                                      neo4jArtifactsPath,
                                      "neo4jserver", 7687,
                                      neo4j_config["edition"])
        server.start()
        hostname, port = server.address()

        # Wait until server is listening before running tests
        # Use driver container to check for Neo4j availability since connect
        # will be done from there
        print("Waiting for neo4j service port to be available")
        driverContainer.exec([
            "python3", "/testkit/driver/wait_for_port.py",
            hostname, "%d" % port])
        print("Neo4j listens")

        # Run the actual test suite within the runner container. The tests
        # will connect to driver backend and configure drivers to connect to
        # the neo4j instance.
        runnerEnv.update({
            # Hostname of Docker container running db
            "TEST_NEO4J_HOST":   hostname,
            "TEST_NEO4J_USER":   neo4j.username,
            "TEST_NEO4J_PASS":   neo4j.password,
        })

        if test_flags["TESTKIT_TESTS"]:
            # Generic integration tests, requires a backend
            suite = neo4j_config["suite"]
            if suite:
                print("Running test suite %s" % suite)
                runnerContainer.exec([
                    "python3", "-m", "tests.neo4j.suites", suite],
                    envMap=runnerEnv)
            else:
                print("No test suite specified for %s" % serverName)

        # Parameters that might be used by native stress/integration
        # tests suites
        driverEnv.update({
            "TEST_NEO4J_HOST":       hostname,
            "TEST_NEO4J_USER":       neo4j.username,
            "TEST_NEO4J_PASS":       neo4j.password,
            "TEST_NEO4J_SCHEME":     neo4j_config["scheme"],
            "TEST_NEO4J_PORT":       port,
            "TEST_NEO4J_EDITION":    neo4j_config["edition"],
            "TEST_NEO4J_VERSION":    neo4j_config["version"],
        })
        if cluster:
            driverEnv["TEST_NEO4J_IS_CLUSTER"] = "1"
        else:
            driverEnv.pop("TEST_NEO4J_IS_CLUSTER", None)

        # To support the legacy .net integration tests
        # TODO: Move this to testkit/driver/dotnet/*.py
        envString = ""
        if neo4j_config["edition"] == "enterprise":
            envString += "-e "
        envString += neo4j_config["version"]
        driverEnv["NEOCTRL_ARGS"] = envString

        # Run the stress test suite within the driver container.
        # The stress test suite uses threading and put a bigger load on the
        # driver than the integration tests do and are therefore written in
        # the driver language.
        # None of the drivers will work properly in cluster.
        if test_flags["STRESS_TESTS"]:
            print("Building and running stress tests...")
            driverContainer.exec([
                "python3", os.path.join(driverGlue, "stress.py")],
                envMap=driverEnv)

        # Run driver native integration tests within the driver container.
        # Driver integration tests should check env variable to skip tests
        # depending on if running in cluster or not, this is not properly done
        # in any (?) driver right now so skip the suite...
        if test_flags["INTEGRATION_TESTS"]:
            if not cluster or driverName in []:
                print("Building and running integration tests...")
                driverContainer.exec([
                    "python3", os.path.join(driverGlue, "integration.py")],
                    envMap=driverEnv)
            else:
                print("Skipping integration tests for %s" % serverName)

        # Check that all connections to Neo4j has been closed.
        # Each test suite should close drivers, sessions properly so any
        # pending connections detected here should indicate connection leakage
        # in the driver.
        print("Checking that connections are closed to the database")
        driverContainer.exec([
            "python3", "/testkit/driver/assert_conns_closed.py",
            hostname, "%d" % port])

        server.stop()


if __name__ == "__main__":
    parse_command_line(sys.argv)

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
