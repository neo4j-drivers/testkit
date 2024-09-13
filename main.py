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
import waiter
from tests.testenv import in_teamcity

# TODO: Move to docker.py
networks = ["testkit_1", "testkit_2"]

test_flags = {
    "TESTKIT_TESTS": True,
    "UNIT_TESTS": True,
    "INTEGRATION_TESTS": True,
    "STUB_TESTS": True,
    "RUN_SELECTED_TESTS": False,
    "EXTERNAL_TESTKIT_TESTS": False,
    "STRESS_TESTS": True,
    "TLS_TESTS": True
}


def initialise_configurations(settings):
    def generate_config(version, enterprise, cluster, scheme, stress_test):
        assert (cluster and scheme == "neo4j"
                or not cluster and scheme in ("neo4j", "bolt"))
        edition = "enterprise" if enterprise else "community"
        name = "%s-%s%s-%s" % (version, edition,
                               "-cluster" if cluster else "", scheme)
        image = f"neo4j:{version}{'-enterprise' if enterprise else ''}"
        return neo4j.Config(
            name=name,
            image=image,
            version=version,
            edition=edition,
            cluster=cluster,
            suite=version,
            scheme=scheme,
            stress_test_duration=stress_test
        )

    def generate_tc_config(
        version, enterprise, cluster, scheme, stress_test, docker_tag=None
    ):
        if not in_teamcity:
            return None
        assert (cluster and scheme == "neo4j"
                or not cluster and scheme in ("neo4j", "bolt"))
        edition = "enterprise" if enterprise else "community"
        name = "%s-tc-%s%s-%s" % (version, edition,
                                  "-cluster" if cluster else "", scheme)
        version_without_drop = ".".join(version.split(".")[:2])
        if docker_tag is None:
            docker_tag = version_without_drop
        return neo4j.Config(
            name=name,
            image=f"{settings.aws_ecr_uri}:{docker_tag}-{edition}-nightly",
            version=version_without_drop,
            edition=edition,
            cluster=cluster,
            suite=version_without_drop,
            scheme=scheme,
            stress_test_duration=stress_test
        )

    # ATTENTION: make sure to have all configs that use the same neo4j docker
    # image (e.g., all configs with neo4j:4.4-community, then all with
    # neo4j:4.4-enterprise, ...) grouped together. Else, TestKit will download
    # the same image multiple times if `TEST_DOCKER_RMI` is set to `true`.
    configurations = [
        generate_config(version_, enterprise_, cluster_, scheme_, stress_test_)
        for (version_, enterprise_, cluster_, scheme_, stress_test_) in (
            # not officially supported versions
            ("4.2",    True,        False,    "neo4j",  0),
            ("4.3",    True,        False,    "neo4j",  0),
            # official backwards-compatibility
            # LTS version
            ("4.4",    False,       False,    "bolt",   0),
            ("4.4",    False,       False,    "neo4j",  0),
            ("4.4",    True,        False,    "bolt",   0),
            ("4.4",    True,        False,    "neo4j",  0),
            ("4.4",    True,        True,     "neo4j", 90),
            # Selected 5.x versions
            # Oldest 5.x version (BOLT 5.0) would be 5.0.
            # However, that has no tag at dockerhub, so we use 5.1
            # https://github.com/neo4j/docker-neo4j/issues/391
            ("5.1",    True,        True,     "neo4j",  0),
            # Bolt 5.1
            ("5.5",    True,        True,     "neo4j",  0),
            # Bolt 5.2
            ("5.7",    True,        True,     "neo4j",  0),
            # Bolt 5.3
            ("5.9",    True,        True,     "neo4j",  0),
            # Bolt 5.4
            ("5.13",   True,        True,     "neo4j",  0),
        )
    ]
    configurations += [
        generate_tc_config(version_, enterprise_, cluster_, scheme_, stress,
                           docker_tag=docker_tag)
        for (version_, docker_tag, enterprise_, cluster_, scheme_,  stress)
        in (
            # nightly build of official backwards-compatible version
            ("4.4",    "4.4",      True,        True,     "neo4j", 60),
            # latest version
            ("5.dev",  "dev",      False,       False,    "bolt",   0),
            ("5.dev",  "dev",      False,       False,    "neo4j",  0),
            ("5.dev",  "dev",      True,        False,    "bolt",  90),
            ("5.dev",  "dev",      True,        False,    "neo4j",  0),
            ("5.dev",  "dev",      True,        True,     "neo4j", 90),
        )
    ]

    configurations = list(filter(lambda conf: conf is not None,
                                 configurations))
    return configurations


def set_test_flags(requested_list, external_tests, selected_test_list):
    if selected_test_list and selected_test_list[0] or external_tests:
        requested_list = []

    if selected_test_list and selected_test_list[0]:
        requested_list.append("RUN_SELECTED_TESTS")
        os.environ["TEST_SELECTOR"] = selected_test_list[0]
    if external_tests:
        requested_list.append("EXTERNAL_TESTKIT_TESTS")

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
    class SmartFormatter(argparse.HelpFormatter):
        def _split_lines(self, text, width):
            if text.startswith("R|"):  # preserve line breaks
                lines = text[2:].splitlines()
                import textwrap
                return [wrapped
                        for line in lines
                        for wrapped in textwrap.wrap(line, width)]
            return super()._split_lines(text, width)

    # create parser
    parser = argparse.ArgumentParser(formatter_class=SmartFormatter)

    keys = ",  ".join(test_flags.keys())
    tests_help = "Optional space separated list selected from: %s" % keys
    external_help = (
        "R|"
        "Flag to *only* run integration tests with an externally started "
        "database. This flag is not compatible with any other flag.\n\n"
        "Supported environment variables:\n"
        "TEST_NEO4J_SCHEME    Scheme to build the URI when contacting the "
        'Neo4j server, default "bolt"\n'
        "TEST_NEO4J_HOST      Neo4j server host, no default, required\n"
        "TEST_NEO4J_PORT      Neo4j server port, default is 7687\n"
        "TEST_NEO4J_USER      User to access the Neo4j server, default "
        '"neo4j"\n'
        "TEST_NEO4J_PASS      Password to access the Neo4j server, default "
        '"pass"\n'
        'TEST_NEO4J_VERSION   Version of the Neo4j server, default "4.4"\n'
        'TEST_NEO4J_EDITION   Edition ("enterprise", "community", or "aura") '
        'of the Neo4j server, default "enterprise"\n'
        "TEST_NEO4J_CLUSTER   Whether the Neo4j server is a cluster, default "
        '"False"\n'
    )
    servers_help = "Optional space separated list selected from: "
    for config in configurations:
        servers_help += config.name + ", "

    run_only_help = (
        "Runs only the selected tests "
        "(see https://docs.python.org/3/library/unittest.html#command-line-interface)"  # noqa: 501
    )

    # add arguments
    parser.add_argument("--tests", nargs="*", required=False,
                        help=tests_help)
    parser.add_argument("--configs", nargs="*", required=False,
                        help=servers_help)
    parser.add_argument("--external-integration", action="store_true",
                        help=external_help)
    parser.add_argument("--run-only-selected", nargs=1, required=False,
                        help=run_only_help)

    # parse the arguments
    args = parser.parse_args()
    set_test_flags(args.tests, args.external_integration,
                   args.run_only_selected)
    configs = construct_configuration_list(configurations, args.configs)
    print("Accepted configurations:")
    for item in configs:
        print("     ", item.name)
    return configs


def build_cleanup(settings):
    def cleanup(*_, **__):
        print("cleanup started")
        docker.cleanup(settings)
        for n in networks:
            print('docker network rm "%s"' % n)
            subprocess.run(["docker", "network", "rm", n],
                           check=False, stdout=subprocess.DEVNULL,
                           stderr=subprocess.DEVNULL)

    return cleanup


def is_stub_test_selected_to_run():
    return (test_flags["RUN_SELECTED_TESTS"]
            and get_selected_tests().startswith("tests.stub"))


def is_neo4j_test_selected_to_run():
    return (test_flags["RUN_SELECTED_TESTS"]
            and get_selected_tests().startswith("tests.neo4j"))


def is_tls_test_selected_to_run():
    return (test_flags["RUN_SELECTED_TESTS"]
            and get_selected_tests().startswith("tests.tls"))


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

    def _exit():
        if failed:
            return "One or more test suites failed."
        return 0

    this_path = settings.testkit_path
    driver_name = settings.driver_name
    testkit_branch = settings.branch
    driver_repo = settings.driver_repo
    # Prepare collecting of artifacts, collected to ARTIFACTS_DIR
    # (default ./artifcats/)
    artifacts_path = os.path.abspath(
        os.environ.get("ARTIFACTS_DIR", os.path.join(".", "artifacts"))
    )
    driver_build_artifacts_path = os.path.join(artifacts_path, "driver_build")
    runner_build_artifacts_path = os.path.join(artifacts_path, "runner_build")
    backend_artifacts_path = os.path.join(artifacts_path, "driver_backend")
    waiter_artifacts_path = os.path.join(artifacts_path, "waiter")
    waiter_build_artifacts_path = os.path.join(artifacts_path, "waiter_build")
    docker_artifacts_path = os.path.join(artifacts_path, "docker")
    # wipe artifacts path
    try:
        shutil.rmtree(artifacts_path)
    except OSError as e:
        if e.errno != errno.ENOENT:
            raise
    os.makedirs(artifacts_path)
    os.makedirs(driver_build_artifacts_path)
    os.makedirs(runner_build_artifacts_path)
    os.makedirs(backend_artifacts_path)
    os.makedirs(waiter_artifacts_path)
    os.makedirs(waiter_build_artifacts_path)
    os.makedirs(docker_artifacts_path)
    print("Putting artifacts in %s" % artifacts_path)

    # Important to stop all docker images upon exit
    # Also make sure that none of those images are running at this point
    cleanup = build_cleanup(settings)
    atexit.register(cleanup)
    cleanup()

    # Create network to be shared among the instances.
    # The host running this will be gateway on that network, retrieve that
    # address to be able to start services on the network that the driver
    # connects to (stub server and TLS server).
    for network in networks:
        cmd = ["docker", "network", "create", network]
        print(cmd)
        subprocess.run(cmd)

    driver_container = driver.start_container(
        this_path, testkit_branch, driver_name, driver_repo,
        docker_artifacts_path, networks[0], networks[1]
    )

    print("Build driver and test backend in driver container")
    driver_container.build_driver_and_backend(driver_build_artifacts_path)
    print("Finished building driver and test backend")

    if test_flags["UNIT_TESTS"]:
        print(">>> Start test suite: driver's unit tests")
        run_fail_wrapper(driver_container.run_unit_tests)
        print(">>> End test suite: driver's unit tests")

    print("Start test backend in driver container")
    driver_container.start_backend(backend_artifacts_path)
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

    if test_flags["EXTERNAL_TESTKIT_TESTS"]:
        if is_neo4j_test_selected_to_run():
            run_fail_wrapper(
                runner_container.run_selected_neo4j_tests_env_config,
                get_selected_tests()
            )
        else:
            run_fail_wrapper(runner_container.run_neo4j_tests_env_config)

    if not (test_flags["TESTKIT_TESTS"]
            or test_flags["STRESS_TESTS"]
            or test_flags["INTEGRATION_TESTS"]
            or (is_neo4j_test_selected_to_run()
                and not test_flags["EXTERNAL_TESTKIT_TESTS"])):
        # no need to download any snapshots or start any servers
        return _exit()

    waiter_container = waiter.start_container(
        this_path, testkit_branch, networks[0],
        docker_artifacts_path, waiter_build_artifacts_path,
        waiter_artifacts_path,
    )

    """
    Neo4j server test matrix
    """
    # Make an artifacts folder where the database can place it's logs, each
    # time we start a database server we should use a different folder.
    neo4j_artifacts_path = os.path.join(artifacts_path, "neo4j")
    os.makedirs(neo4j_artifacts_path)
    last_image = None
    for neo4j_config in configurations:
        if (
            last_image
            and settings.docker_rmi
            and neo4j_config.image != last_image
        ):
            cmd = ["docker", "rmi", last_image]
            print(cmd)
            subprocess.run(cmd)
        last_image = neo4j_config.image

        cluster = neo4j_config.cluster
        server_name = neo4j_config.name
        stress_duration = neo4j_config.stress_test_duration

        # Start a Neo4j server
        if cluster:
            print("\n    Starting neo4j cluster (%s)\n" % server_name)
            server = neo4j.Cluster(neo4j_config.image,
                                   server_name,
                                   neo4j_artifacts_path,
                                   neo4j_config.version)
        else:
            print("\n    Starting neo4j standalone server (%s)\n"
                  % server_name)
            server = neo4j.Standalone(
                neo4j_config.image, server_name, neo4j_artifacts_path,
                "neo4jserver", 7687, neo4j_config.version, neo4j_config.edition
            )
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
            # Wait some more for server to be ready.
            # Especially starting with 5.0, the server starts the bolt server
            # before it starts the databases. This will mean the port will be
            # available before queries can be executed for clusters and for
            # the enterprise edition in stand-alone mode.
            if int(neo4j_config.version.split(".", 1)[0]) >= 5:
                waiter_container.wait_for_all_dbs(
                    address, neo4j.username, neo4j.password
                )
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
                    hostname, port, neo4j.username, neo4j.password,
                    neo4j_config
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

    if last_image and settings.docker_rmi:
        cmd = ["docker", "rmi", last_image]
        print(cmd)
        subprocess.run(cmd)

    return _exit()


if __name__ == "__main__":
    # Retrieve path to the repository containing this script.
    # Use this path as base for locating a whole bunch of other stuff.
    # Add this path to python sys path to be able to invoke modules
    # from this repo
    this_path = os.path.dirname(os.path.abspath(__file__))
    os.environ["PYTHONPATH"] = this_path
    try:
        settings = settings.build(this_path)
    except settings.ArgumentError as e:
        print("")
        print(e)
        sys.exit(-1)

    # setup the configurations that are available
    configurations = initialise_configurations(settings)
    configurations = parse_command_line(configurations, sys.argv)

    sys.exit(main(settings, configurations))
