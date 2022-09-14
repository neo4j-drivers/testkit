"""Retrieves the information needed to run main, stress or other test suite."""

import collections
import os


class ArgumentError(Exception):
    pass


Settings = collections.namedtuple("Settings", [
    "in_teamcity", "driver_name", "branch", "testkit_path", "driver_repo",
    "run_all_tests", "docker_rmi"
])


def _get_env_bool(name):
    return os.environ.get(name, "").lower() in ("true", "y", "yes", "1", "on")


def build(testkit_path):
    """Build. the context based environment variables."""
    in_teamcity = (os.environ.get("TEST_IN_TEAMCITY", "").upper()
                   in ("TRUE", "1", "Y", "YES", "ON"))

    # Retrieve path to driver git repository
    driver_repo = os.environ.get("TEST_DRIVER_REPO")
    if not driver_repo:
        raise ArgumentError(
            "Missing environment variable TEST_DRIVER_REPO that contains "
            "path to driver repository"
        )

    driver_name = os.environ.get("TEST_DRIVER_NAME")
    if not driver_name:
        raise ArgumentError(
            "Missing environment variable TEST_DRIVER_NAME that contains "
            "name of the driver"
        )

    branch = os.environ.get("TEST_BRANCH")
    if not branch:
        if in_teamcity:
            raise ArgumentError(
                "Missing environment variable TEST_BRANCH that contains "
                "name of testkit branch. "
                "This name is used to name Docker repository."
            )
        branch = "local"

    run_all_tests = _get_env_bool("TEST_RUN_ALL_TESTS")

    docker_rmi = _get_env_bool("TEST_DOCKER_RMI")

    return Settings(**locals())
