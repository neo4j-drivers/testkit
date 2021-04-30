"""
Runs all test suites in their appropriate context for a given driver.

Brings up different versions of Neo4j server using Docker and runs the driver
test suites against the instance.

The same test suite might be executed on different Neo4j server versions or
setups, it is the responsibility of this script to setup these contexts and
orchestrate which suites that are executed in each context.
"""

import argparse
import atexit
import os
import subprocess
import sys
import traceback

import docker
import driver
import neo4j
import runner
import settings
import teamcity
from tests.testenv import (
    begin_test_suite,
    end_test_suite,
    in_teamcity,
)


# TODO: Move to docker.py
networks = ["testkit_1", "testkit_2"]

test_flags = {
    "TESTKIT_TESTS": True,
    "UNIT_TESTS": True,
    "INTEGRATION_TESTS": True,
    "STUB_TESTS": True,
    "STRESS_TESTS": True,
    "TLS_TESTS": True
}


def initialise_configurations():
    configurations = []
    configurations.append(neo4j.Config(
        name="4.2-cluster",
        image="neo4j:4.2-enterprise",
        version="4.2",
        edition="enterprise",
        cluster=True,
        suite="",  # TODO: Define cluster suite
        scheme="neo4j",
        download=None,
        stress_test_duration=90))

    configurations.append(neo4j.Config(
        name="3.5-enterprise",
        image="neo4j:3.5-enterprise",
        version="3.5",
        edition="enterprise",
        cluster=False,
        suite="3.5",
        scheme="bolt",
        download=None,
        stress_test_duration=0))

    configurations.append(neo4j.Config(
        name="4.0-community",
        version="4.0",
        image="neo4j:4.0",
        edition="community",
        cluster=False,
        suite="4.0",
        scheme="neo4j",
        download=None,
        stress_test_duration=0))

    configurations.append(neo4j.Config(
        name="4.1-enterprise",
        image="neo4j:4.1-enterprise",
        version="4.1",
        edition="enterprise",
        cluster=False,
        suite="4.1",
        scheme="neo4j",
        download=None,
        stress_test_duration=0))

    if in_teamcity:
        configurations.append(neo4j.Config(
            name="4.2-tc-enterprise",
            image="neo4j:4.2.3-enterprise",
            version="4.2",
            edition="enterprise",
            cluster=False,
            suite="4.2",
            scheme="neo4j",
            download=teamcity.DockerImage(
                "neo4j-enterprise-4.2.4-docker-loadable.tar"),
            stress_test_duration=0))

        configurations.append(neo4j.Config(
            name="4.3-tc-enterprise",
            image="neo4j:4.3.0-drop03.0-enterprise",
            version="4.3",
            edition="enterprise",
            cluster=False,
            suite="4.3",
            scheme="neo4j",
            download=teamcity.DockerImage(
                "neo4j-enterprise-4.3.0-drop03.0-docker-loadable.tar"),
            stress_test_duration=0))
    return configurations


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


def construct_configuration_list(configurations, requested_list):
    # if no configs were requested we will default to running them all
    if not requested_list:
        return configurations

    # Now try to find the requested configs and check they are available
    # with current teamcity status
    configs = []
    for config in configurations:
        if config.name in requested_list:
            configs.append(config)
    return configs


def parse_command_line(configurations, argv):
    # create parser
    parser = argparse.ArgumentParser()

    keys = ",  ".join(test_flags.keys())
    tests_help = "Optional space separated list selected from: %s" % keys
    servers_help = "Optional space separated list selected from: "
    for config in configurations:
        servers_help += config.name + ", "

    # add arguments
    parser.add_argument(
            "--tests", nargs='*', required=False, help=tests_help)
    parser.add_argument(
            "--configs", nargs='*', required=False, help=servers_help)

    # parse the arguments
    args = parser.parse_args()

    set_test_flags(args.tests)
    configs = construct_configuration_list(configurations, args.configs)
    print("Accepted configurations:")
    for item in configs:
        print("     ", item.name)
    return configs


def cleanup(*_, **__):
    print("cleanup started")
    docker.cleanup()
    for n in networks:
        print('docker network rm "%s"' % n)
        subprocess.run(["docker", "network", "rm", n],
                       check=False, stdout=subprocess.DEVNULL,
                       stderr=subprocess.DEVNULL)


def main(settings, configurations):
    failed = False

    def run_fail_wrapper(func, *args, **kwargs):
        nonlocal failed
        try:
            func(*args, **kwargs)
        except subprocess.CalledProcessError:
            failed = True
            if settings.run_all_tests:
                traceback.print_exc()
            else:
                raise

    this_path = settings.testkit_path
    driver_name = settings.driver_name
    testkit_branch = settings.branch
    driver_repo = settings.driver_repo
    #  this_path, driver_name, testkit_branch, driver_repo):
    # Prepare collecting of artifacts, collected to ./artifcats/
    artifacts_path = os.path.abspath(os.path.join(".", "artifacts"))
    if not os.path.exists(artifacts_path):
        os.makedirs(artifacts_path)
    print("Putting artifacts in %s" % artifacts_path)

    # Important to stop all docker images upon exit
    # Also make sure that none of those images are running at this point
    atexit.register(cleanup)
    cleanup()

    # Create network to be shared among the instances.
    # The host running this will be gateway on that network, retrieve that
    # address to be able to start services on the network that the driver
    # connects to (stub server and TLS server).
    subprocess.run([
        "docker", "network", "create", networks[0]
    ])
    subprocess.run([
        "docker", "network", "create", networks[1]
    ])

    driver_container = driver.start_container(this_path, testkit_branch,
                                              driver_name, driver_repo,
                                              artifacts_path,
                                              network=networks[0], secondary_network=networks[1])
    driver_container.clean_artifacts()
    print("Cleaned up artifacts")

    print("Build driver and test backend in driver container")
    driver_container.build_driver_and_backend()
    print("Finished building driver and test backend")

    if test_flags["UNIT_TESTS"]:
        begin_test_suite('Unit tests')
        run_fail_wrapper(driver_container.run_unit_tests)
        end_test_suite('Unit tests')

    print("Start test backend in driver container")
    driver_container.start_backend()
    print("Started test backend")

    # Start runner container, responsible for running the unit tests.
    runner_container = runner.start_container(this_path, testkit_branch,
                                              network=networks[0], secondary_network=networks[1])

    if test_flags["STUB_TESTS"]:
        run_fail_wrapper(runner_container.run_stub_tests)

    if test_flags["TLS_TESTS"]:
        run_fail_wrapper(runner_container.run_tls_tests)

    if not (test_flags["TESTKIT_TESTS"]
            or test_flags["STRESS_TESTS"]
            or test_flags["INTEGRATION_TESTS"]):
        # no need to download any snapshots or start any servers
        return

    """
    Neo4j server test matrix
    """
    # Make an artifacts folder where the database can place it's logs, each
    # time we start a database server we should use a different folder.
    neo4j_artifacts_path = os.path.join(artifacts_path, "neo4j")
    os.makedirs(neo4j_artifacts_path)
    for neo4j_config in configurations:
        download = neo4j_config.download
        if download:
            print("Downloading Neo4j docker image")
            docker.load(download.get())

        cluster = neo4j_config.cluster
        server_name = neo4j_config.name
        stress_duration = neo4j_config.stress_test_duration

        # Start a Neo4j server
        if cluster:
            print("Starting neo4j cluster (%s)" % server_name)
            server = neo4j.Cluster(neo4j_config.image,
                                   server_name,
                                   neo4j_artifacts_path)
        else:
            print("Starting neo4j standalone server (%s)" % server_name)
            server = neo4j.Standalone(neo4j_config.image,
                                      server_name,
                                      neo4j_artifacts_path,
                                      "neo4jserver", 7687,
                                      neo4j_config.edition)
        server.start(networks[0])
        hostname, port = server.address()

        # Wait until server is listening before running tests
        # Use driver container to check for Neo4j availability since connect
        # will be done from there
        print("Waiting for neo4j service port to be available")
        driver_container.poll_host_and_port_until_available(hostname, port)
        print("Neo4j is reachable from driver")

        if test_flags["TESTKIT_TESTS"]:
            # Generic integration tests, requires a backend
            suite = neo4j_config.suite
            if suite:
                print("Running test suite %s" % suite)
                run_fail_wrapper(
                    runner_container.run_neo4j_tests,
                    suite, hostname, neo4j.username, neo4j.password
                )
            else:
                print("No test suite specified for %s" % server_name)

        # Run the stress test suite within the driver container.
        # The stress test suite uses threading and put a bigger load on the
        # driver than the integration tests do and are therefore written in
        # the driver language.
        if test_flags["STRESS_TESTS"] and stress_duration > 0:
            print("Building and running stress tests...")
            run_fail_wrapper(
                driver_container.run_stress_tests,
                hostname, port, neo4j.username, neo4j.password, neo4j_config
            )

        # Run driver native integration tests within the driver container.
        # Driver integration tests should check env variable to skip tests
        # depending on if running in cluster or not, this is not properly done
        # in any (?) driver right now so skip the suite...
        if test_flags["INTEGRATION_TESTS"]:
            if not cluster:
                print("Building and running integration tests...")
                run_fail_wrapper(
                    driver_container.run_integration_tests,
                    hostname, port, neo4j.username, neo4j.password, neo4j_config
                )
            else:
                print("Skipping integration tests for %s" % server_name)

        # Check that all connections to Neo4j has been closed.
        # Each test suite should close drivers, sessions properly so any
        # pending connections detected here should indicate connection leakage
        # in the driver.
        print("Checking that connections are closed to the database")
        driver_container.assert_connections_closed(hostname, port)

        server.stop()

    if failed:
        sys.exit("One or more test suites failed.")


if __name__ == "__main__":
    # setup the configurations that are available
    configurations = initialise_configurations()
    configurations = parse_command_line(configurations, sys.argv)

    # Retrieve path to the repository containing this script.
    # Use this path as base for locating a whole bunch of other stuff.
    # Add this path to python sys path to be able to invoke modules
    # from this repo
    this_path = os.path.dirname(os.path.abspath(__file__))
    os.environ['PYTHONPATH'] = this_path

    try:
        settings = settings.build(this_path)
    except settings.InvalidArgs as e:
        print('')
        print(e)
        sys.exit(-1)

    main(settings, configurations)
