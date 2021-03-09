import os
import subprocess

import docker


def _ensure_image(testkit_path, branch_name):
    """ Ensures that an up to date Docker image exists.
    """
    # Construct Docker image name from branch name
    image_name = "runner:%s" % branch_name
    image_path = os.path.join(testkit_path, "runner_image")
    print("Building runner Docker image %s from %s" % (image_name, image_path))
    subprocess.check_call([
        "docker", "build", "--tag", image_name, image_path])
    docker.remove_dangling()

    return image_name


def start_container(testkit_path, branch_name):
    image = _ensure_image(testkit_path, branch_name)
    env = {
        # Runner connects to backend in driver container
        "TEST_BACKEND_HOST": "driver",
        # Driver connects to me
        "TEST_STUB_HOST":    "runner",
        # To use modules
        "PYTHONPATH":        "/testkit",
    }
    # Copy TEST_ variables that might have been set explicit
    for varName in os.environ:
        if varName.startswith("TEST_"):
            env[varName] = os.environ[varName]
    container = docker.run(
            image, "runner",
            command=["python3", "/testkit/driver/bootstrap.py"],
            mount_map={testkit_path: "/testkit"},
            env_map=env,
            network="the-bridge",
            aliases=["thehost", "thehostbutwrong"])  # Used when testing TLS
    return Container(container, env)


class Container:
    def __init__(self, container, env):
        self._container = container
        self._env = env

    def run_stub_tests(self):
        self._container.exec(["python3", "-m", "tests.stub.suites"])

    def run_tls_tests(self):
        # Build TLS server
        self._container.exec(
                ["go", "build", "-v", "."], workdir="/testkit/tlsserver")
        self._container.exec(
                ["python3", "-m", "tests.tls.suites"])

    def run_neo4j_tests(self, suite, hostname, username, password):
        self._env.update({
            # Hostname of Docker container running db
            "TEST_NEO4J_HOST": hostname,
            "TEST_NEO4J_USER": username,
            "TEST_NEO4J_PASS": password,
        })
        self._container.exec([
            "python3", "-m", "tests.neo4j.suites", suite],
            env_map=self._env)
