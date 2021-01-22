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
import argparse

from tests.testenv import (
        begin_test_suite, end_test_suite, in_teamcity)
import docker
import teamcity
import neo4j
import driver
import runner
import settings

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
    configurations.append({
        "name": "4.2-cluster",
        "image": "neo4j:4.2-enterprise",
        "version": "4.2",
        "edition": "enterprise",
        "cluster": True,
        "suite": "",  # TODO: Define cluster suite
        "scheme": "neo4j"})

    configurations.append({
        "name": "3.5-enterprise",
        "image": "neo4j:3.5-enterprise",
        "version": "3.5",
        "edition": "enterprise",
        "cluster": False,
        "suite": "3.5",
        "scheme": "bolt"})

    configurations.append({
        "name": "4.0-community",
        "version": "4.0",
        "image": "neo4j:4.0",
        "edition": "community",
        "cluster": False,
        "suite": "4.0",
        "scheme": "neo4j"})

    configurations.append({
        "name": "4.1-enterprise",
        "image": "neo4j:4.1-enterprise",
        "version": "4.1",
        "edition": "enterprise",
        "cluster": False,
        "suite": "4.1",
        "scheme": "neo4j"})

    if in_teamcity:
        configurations.append({
            "name": "4.2-tc-enterprise",
            "image": "neo4j:4.2.3-enterprise",
            "version": "4.2",
            "edition": "enterprise",
            "cluster": False,
            "suite": "4.2",
            "scheme": "neo4j",
            "download": teamcity.DockerImage(
                "neo4j-enterprise-4.2.3-docker-loadable.tar")})

        configurations.append({
            "name": "4.3-tc-enterprise",
            "image": "neo4j:4.3.0-drop02.0-enterprise",
            "version": "4.3",
            "edition": "enterprise",
            "cluster": False,
            "suite": "4.3",
            "scheme": "neo4j",
            "download": teamcity.DockerImage(
                "neo4j-enterprise-4.3.0-drop02.0-docker-loadable.tar")})


def set_test_flags(requested_list):
    if not requested_list:
        requested_list = test_flags

    for item in test_flags:
        if item not in requested_list:
            test_flags[item] = False

    print("Tests that will be run:")
    for item in test_flags:
        if test_flags[item]:
            print("     ", item)


def construct_configuration_list(requested_list):
    # if no configs were requested we will default to adding them all
    if not requested_list:
        requested_list = []
        for config in configurations:
            requested_list.append(config["name"])

    # Now try to find the requested configs and check they are available
    # with current teamcity status
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

    keys = ",  ".join(test_flags.keys())
    tests_help = "Optional space separated list selected from: %s" % keys
    servers_help = "Optional space separated list selected from: "
    for config in configurations:
        servers_help += config["name"] + ", "

    # add arguments
    parser.add_argument(
            "--tests", nargs='*', required=False, help=tests_help)
    parser.add_argument(
            "--configs", nargs='*', required=False, help=servers_help)

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


def main(settings):
    thisPath = settings.testkit_path
    driverName = settings.driver_name
    testkitBranch = settings.branch
    driverRepo = settings.driver_repo
    #  thisPath, driverName, testkitBranch, driverRepo):
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

    driverContainer = driver.start_container(thisPath, testkitBranch,
                                             driverName, driverRepo,
                                             artifactsPath)
    driverContainer.clean_artifacts()
    print("Cleaned up artifacts")

    print("Build driver and test backend in driver container")
    driverContainer.build_driver_and_backend()
    print("Finished building driver and test backend")

    if test_flags["UNIT_TESTS"]:
        begin_test_suite('Unit tests')
        driverContainer.run_unit_tests()
        end_test_suite('Unit tests')

    print("Start test backend in driver container")
    driverContainer.start_backend()
    print("Started test backend")

    # Start runner container, responsible for running the unit tests.
    runnerContainer = runner.start_container(thisPath, testkitBranch)

    if test_flags["STUB_TESTS"]:
        runnerContainer.run_stub_tests()

    if test_flags["TLS_TESTS"]:
        runnerContainer.run_tls_tests()

    """
    Neo4j server test matrix
    """
    # Make an artifacts folder where the database can place it's logs, each
    # time we start a database server we should use a different folder.
    neo4jArtifactsPath = os.path.join(artifactsPath, "neo4j")
    os.makedirs(neo4jArtifactsPath)
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
        driverContainer.poll_host_and_port_until_available(hostname, port)
        print("Neo4j is reachable from driver")

        if test_flags["TESTKIT_TESTS"]:
            # Generic integration tests, requires a backend
            suite = neo4j_config["suite"]
            if suite:
                print("Running test suite %s" % suite)
                runnerContainer.run_neo4j_tests(suite, hostname,
                                                neo4j.username,
                                                neo4j.password)
            else:
                print("No test suite specified for %s" % serverName)

        # Run the stress test suite within the driver container.
        # The stress test suite uses threading and put a bigger load on the
        # driver than the integration tests do and are therefore written in
        # the driver language.
        if test_flags["STRESS_TESTS"]:
            print("Building and running stress tests...")
            driverContainer.run_stress_tests(hostname, port, neo4j.username,
                                             neo4j.password, neo4j_config)

        # Run driver native integration tests within the driver container.
        # Driver integration tests should check env variable to skip tests
        # depending on if running in cluster or not, this is not properly done
        # in any (?) driver right now so skip the suite...
        if test_flags["INTEGRATION_TESTS"]:
            if not cluster:
                print("Building and running integration tests...")
                driverContainer.run_integration_tests(hostname, port,
                                                      neo4j.username,
                                                      neo4j.password,
                                                      neo4j_config)
            else:
                print("Skipping integration tests for %s" % serverName)

        # Check that all connections to Neo4j has been closed.
        # Each test suite should close drivers, sessions properly so any
        # pending connections detected here should indicate connection leakage
        # in the driver.
        print("Checking that connections are closed to the database")
        driverContainer.assert_connections_closed(hostname, port)

        server.stop()


if __name__ == "__main__":
    parse_command_line(sys.argv)

    # Retrieve path to the repository containing this script.
    # Use this path as base for locating a whole bunch of other stuff.
    # Add this path to python sys path to be able to invoke modules
    # from this repo
    thisPath = os.path.dirname(os.path.abspath(__file__))
    os.environ['PYTHONPATH'] = thisPath

    try:
        settings = settings.build(thisPath)
    except settings.InvalidArgs as e:
        print('')
        print(e)
        os.sys.exit(-1)

    main(settings)
