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
import errno
import os
import shutil
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
    "RUN_SELECTED_TESTS": False,
    "STRESS_TESTS": True,
    "TLS_TESTS": True
}


def initialise_configurations():
    def generate_config(version, enterprise, cluster, scheme, stress_test):
        assert (cluster and scheme == "neo4j"
                or not cluster and scheme in ("neo4j", "bolt"))
        edition = "enterprise" if enterprise else "community"
        name = "%s-%s%s-%s" % (version, edition,
                               "-cluster" if cluster else "", scheme)
        return neo4j.Config(
            name=name,
            image="neo4j:%s%s" % (version, "-enterprise" if enterprise else ""),
            version=version,
            edition=edition,
            cluster=cluster,
            suite=version,
            scheme=scheme,
            download=None,
            stress_test_duration=stress_test
        )

    def generate_tc_config(version, enterprise, cluster, scheme, stress_test):
        if not in_teamcity:
            return None
        assert (cluster and scheme == "neo4j"
                or not cluster and scheme in ("neo4j", "bolt"))
        edition = "enterprise" if enterprise else "community"
        name = "%s-tc-%s%s-%s" % (version, edition,
                                  "-cluster" if cluster else "", scheme)
        version_without_drop = ".".join(version.split(".")[:2])
        return neo4j.Config(
            name=name,
            image="neo4j:%s%s" % (version, "-enterprise" if enterprise else ""),
            version=version_without_drop,
            edition=edition,
            cluster=cluster,
            suite=version_without_drop,
            scheme=scheme,
            download=teamcity.DockerImage(
                "neo4j-%s-%s-docker-loadable.tar" % (edition, version)
            ),
            stress_test_duration=stress_test
        )

    configurations = [
        generate_config(version_, enterprise_, cluster_, scheme_, stress_test_)
        for (version_, enterprise_, cluster_, scheme_, stress_test_) in (
            # LTS version
            # 3.5 servers only support routing scheme if configured as cluster
            ("3.5",    False,       False,    "bolt",   0),
            ("3.5",    True,        False,    "bolt",   0),
            ("3.5",    True,        True,     "neo4j", 60),
            # not officially supported versions
            ("4.0",    True,        False,    "neo4j",  0),
            ("4.1",    True,        False,    "neo4j",  0),
            ("4.2",    True,        False,    "neo4j",  0),
            # official backwards-compatibility
            ("4.3",    False,       False,    "bolt",   0),
            ("4.3",    False,       False,    "neo4j",  0),
            ("4.3",    True,        False,    "bolt",   0),
            ("4.3",    True,        False,    "neo4j",  0),
            ("4.3",    True,        True,     "neo4j", 90),
        )
    ]
    configurations += [
        generate_tc_config(version_, enterprise_, cluster_, scheme_,
                           stress_test_)
        for (version_, enterprise_, cluster_, scheme_, stress_test_) in (
            # nightly build of official backwards-compatible version
            ("4.3.6",     True,     True,     "neo4j", 60),
            # latest version
            ("4.4.0-dev", False,    False,    "bolt",   0),
            ("4.4.0-dev", False,    False,    "neo4j",  0),
            ("4.4.0-dev", True,     False,    "bolt",  90),
            ("4.4.0-dev", True,     False,    "neo4j",  0),
            ("4.4.0-dev", True,     True,     "neo4j", 90),
        )
    ]

    configurations = list(filter(lambda conf: conf is not None, configurations))
    return configurations


def set_test_flags(requested_list, selected_test_list):
    if selected_test_list and selected_test_list[0]:
        requested_list = ['RUN_SELECTED_TESTS']
        os.environ['TEST_SELECTOR'] = selected_test_list[0]

    if requested_list:
        for item in test_flags:
            test_flags[item] = item in requested_list

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

    run_only_help = "Runs only the selected tests (see https://docs.python.org/3/library/unittest.html#command-line-interface)"

    # add arguments
    parser.add_argument(
        "--tests", nargs='*', required=False, help=tests_help)
    parser.add_argument(
        "--configs", nargs='*', required=False, help=servers_help)
    parser.add_argument(
        "--run-only-selected", nargs=1, required=False, help=run_only_help)

    # parse the arguments
    args = parser.parse_args()
    set_test_flags(args.tests, args.run_only_selected)
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


def is_stub_test_selected_to_run():
    return (
        test_flags["RUN_SELECTED_TESTS"] and
        get_selected_tests().startswith("tests.stub")
    )


def is_neo4j_test_selected_to_run():
    return (
        test_flags["RUN_SELECTED_TESTS"] and
        get_selected_tests().startswith("tests.neo4j")
    )


def is_tls_test_selected_to_run():
    return (
        test_flags["RUN_SELECTED_TESTS"] and
        get_selected_tests().startswith("tests.tls")
    )


def get_selected_tests():
    return os.environ.get("TEST_SELECTOR", "")


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
    # Prepare collecting of artifacts, collected to ARTIFACTS_DIR
    # (default ./artifcats/)
    artifacts_path = os.path.abspath(
        os.environ.get("ARTIFACTS_DIR", os.path.join(".", "artifacts"))
    )
    driver_run_artifacts_path = os.path.join(artifacts_path, "driver_run")
    driver_build_artifacts_path = os.path.join(artifacts_path, "driver_build")
    runner_build_artifacts_path = os.path.join(artifacts_path, "runner_build")
    docker_artifacts_path = os.path.join(artifacts_path, "docker")
    # wipe artifacts path
    try:
        shutil.rmtree(artifacts_path)
    except OSError as e:
        if e.errno != errno.ENOENT:
            raise
    os.makedirs(artifacts_path)
    os.makedirs(driver_run_artifacts_path)
    os.makedirs(driver_build_artifacts_path)
    os.makedirs(runner_build_artifacts_path)
    os.makedirs(docker_artifacts_path)
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

    driver_container = driver.start_container(
        this_path, testkit_branch, driver_name, driver_repo,
        driver_run_artifacts_path, docker_artifacts_path,
        networks[0], networks[1]
    )

    print("Build driver and test backend in driver container")
    driver_container.build_driver_and_backend(driver_build_artifacts_path)
    print("Finished building driver and test backend")

    if test_flags["UNIT_TESTS"]:
        begin_test_suite('Unit tests')
        run_fail_wrapper(driver_container.run_unit_tests)
        end_test_suite('Unit tests')

    print("Start test backend in driver container")
    driver_container.start_backend()
    print("Started test backend")

    # Start runner container, responsible for running the unit tests.
    runner_container = runner.start_container(
        this_path, testkit_branch,
        networks[0], networks[1],
        docker_artifacts_path, runner_build_artifacts_path
    )

    if test_flags["STUB_TESTS"]:
        run_fail_wrapper(runner_container.run_stub_tests)

    if test_flags["TLS_TESTS"]:
        run_fail_wrapper(runner_container.run_tls_tests)

    # Running selected STUB tests
    if is_stub_test_selected_to_run():
        run_fail_wrapper(
            runner_container.run_selected_stub_tests,
            get_selected_tests()
        )

    # Running selected TLS tests
    if is_tls_test_selected_to_run():
        run_fail_wrapper(
            runner_container.run_selected_tls_tests,
            get_selected_tests()
        )

    if not (test_flags["TESTKIT_TESTS"]
            or test_flags["STRESS_TESTS"]
            or test_flags["INTEGRATION_TESTS"]
            or is_neo4j_test_selected_to_run()):
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
            print("\n    Starting neo4j cluster (%s)\n" % server_name)
            server = neo4j.Cluster(neo4j_config.image,
                                   server_name,
                                   neo4j_artifacts_path)
        else:
            print("\n    Starting neo4j standalone server (%s)\n" % server_name)
            server = neo4j.Standalone(neo4j_config.image,
                                      server_name,
                                      neo4j_artifacts_path,
                                      "neo4jserver", 7687,
                                      neo4j_config.edition)
        server.start(networks[0])
        addresses = server.addresses()
        hostname, port = addresses[0]

        # Wait until server is listening before running tests
        # Use driver container to check for Neo4j availability since connect
        # will be done from there
        for address in addresses:
            print("Waiting for neo4j service at %s to be available"
                  % (address,))
            driver_container.poll_host_and_port_until_available(*address)
        print("Neo4j is reachable from driver")

        if test_flags["TESTKIT_TESTS"]:
            # Generic integration tests, requires a backend
            suite = neo4j_config.suite
            if suite:
                print("Running test suite %s" % suite)
                run_fail_wrapper(
                    runner_container.run_neo4j_tests,
                    suite, hostname, neo4j.username, neo4j.password,
                    neo4j_config
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

        # Running selected NEO4J tests
        if is_neo4j_test_selected_to_run():
            run_fail_wrapper(
                runner_container.run_selected_neo4j_tests,
                get_selected_tests(), hostname, neo4j.username, neo4j.password,
                neo4j_config
            )

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
