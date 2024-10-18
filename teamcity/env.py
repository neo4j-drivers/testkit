import os


def evaluate_env_variable():
    return (os.environ.get("TEST_IN_TEAMCITY", "").upper()
            in ("TRUE", "1", "Y", "YES", "ON"))


in_teamcity = evaluate_env_variable()
