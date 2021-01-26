"""
Runs the stress test suite for a given driver against a running database
instance.

Builds the driver in the drivers Docker container and invokes the driver
native stress test suite.
"""
import atexit
import os
import urllib.parse

import docker
import driver
import settings
import neo4j


def run(settings):
    artifacts_path = os.path.abspath(
            os.path.join(".", "artifacts"))
    print("Putting artifacts in %s" % artifacts_path)
    atexit.register(docker.cleanup)

    driver_container = driver.start_container(settings.testkit_path,
                                              settings.branch,
                                              settings.driver_name,
                                              settings.driver_repo,
                                              artifacts_path)
    driver_container.clean_artifacts()
    print("Building driver")
    driver_container.build_driver_and_backend()

    # Retrieve info about the Neo4j database from environment
    # Use same naming here as we use in communication with the
    # driver glue in Docker container (a bit weird to read them,
    # package them and then read again by another part...)
    user = os.environ.get('TEST_NEO4J_USER', "neo4j")
    password = os.environ.get('TEST_NEO4J_PASS')
    uri = os.environ.get("TEST_NEO4J_URI")
    if uri:
        # Split in parts...
        parts = urllib.parse.urlsplit(uri)
        scheme = parts[0]
        host = parts[1]  # Might include port
        port = parts.port if parts.port else ""
    else:
        # Retrieve the individual parts
        host = os.environ.get("TEST_NEO4J_HOST")
        port = os.environ.get("TEST_NEO4J_PORT")
        scheme = os.environ.get("TEST_NEO4J_SCHEME")

    if not port:
        port = "7687"

    neo4j_config = neo4j.Config(
        name="Custom stress",
        image="",
        version=os.environ.get("TEST_NEO4J_VERSION", "drop"),
        edition=os.environ.get("TEST_NEO4J_EDITION"),
        cluster=os.environ.get("TEST_NEO4J_IS_CLUSTER", True),
        suite="",
        scheme=scheme,
        stress_test_duration=10*60,
        download=None)
    print("Running stress test suite..")
    driver_container.run_stress_tests(host, port,
                                      user, password,
                                      neo4j_config)


if __name__ == "__main__":
    this_path = os.path.dirname(os.path.abspath(__file__))
    try:
        settings = settings.build(this_path)
    except settings.InvalidArgs as e:
        print('')
        print(e)
        os.sys.exit(-1)
    run(settings)
