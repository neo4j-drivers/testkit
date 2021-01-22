"""
Retrieves the information needed to run main, stress or other test suite.
"""
import os
import collections


class InvalidArgs(Exception):
    pass


Settings = collections.namedtuple('Settings', [
    'in_teamcity', 'driver_name', 'branch', 'testkit_path', 'driver_repo'])


def build(testkit_path):
    """ Builds the context based on what is defined in environment variables.
    """
    in_teamcity = os.environ.get('TEST_IN_TEAMCITY', '').upper() in [
            'TRUE', '1']

    # Retrieve path to driver git repository
    driver_repo = os.environ.get("TEST_DRIVER_REPO")
    if not driver_repo:
        raise InvalidArgs(
                "Missing environment variable TEST_DRIVER_REPO that contains "
                "path to driver repository")

    driver_name = os.environ.get("TEST_DRIVER_NAME")
    if not driver_name:
        raise InvalidArgs(
                "Missing environment variable TEST_DRIVER_NAME that contains "
                "name of the driver")

    branch = os.environ.get("TEST_BRANCH")
    if not branch:
        if in_teamcity:
            raise InvalidArgs(
                "Missing environment variable TEST_BRANCH that contains "
                "name of testkit branch. "
                "This name is used to name Docker repository.")
        branch = "local"

    return Settings(**locals())
