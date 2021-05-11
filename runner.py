import os

import docker


def _ensure_image(testkit_path, branch_name):
    """ Ensures that an up to date Docker image exists.
    """
    # Construct Docker image name from branch name
    image_name = "runner:%s" % branch_name
    image_path = os.path.join(testkit_path, "runner_image")
    docker.build_and_tag(image_name, image_path)

    return image_name


def start_container(testkit_path, branch_name, network, secondary_network):
    image = _ensure_image(testkit_path, branch_name)
    container_name = "runner"
    env = {
        # Runner connects to backend in driver container
        "TEST_BACKEND_HOST": "driver",
        # Driver connects to me
        "TEST_STUB_HOST": container_name,
        # To use modules
        "PYTHONPATH": "/testkit",
    }
    # Copy TEST_ variables that might have been set explicit
    for varName in os.environ:
        if varName.startswith("TEST_"):
            env[varName] = os.environ[varName]
    docker.create(
        image, "runner",
        command=["python3", "/testkit/driver/bootstrap.py"],
        mount_map={testkit_path: "/testkit"},
        env_map=env,
        network=network,
        aliases=["thehost", "thehostbutwrong"])  # Used when testing TLS
    docker.network_connect(secondary_network, container_name)
    container = docker.start(container_name)
    return Container(container, env)


class Container:
    def __init__(self, container, env):
        self._container = container
        self._env = env
        self._init_container()

    def _init_container(self):
        self._container.exec(["pip3", "install", "-U", "pip"])
        self._container.exec(["pip3", "install", "-Ur",
                              "/testkit/requirements.txt"])

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

    def run_selected_stub_tests(self, testpattern):
        self._container.exec(["python3", "-m", "unittest", "-v", testpattern])

    def run_selected_tls_tests(self, testpattern):
        # Build TLS server
        self._container.exec(
                ["go", "build", "-v", "."], workdir="/testkit/tlsserver")
        self._container.exec(["python3", "-m", "unittest", "-v", testpattern])

    def run_selected_neo4j_tests(
            self, testpattern, hostname, username, password):
        self._env.update({
            # Hostname of Docker container running db
            "TEST_NEO4J_HOST": hostname,
            "TEST_NEO4J_USER": username,
            "TEST_NEO4J_PASS": password,
        })
        self._container.exec([
            "python3", "-m", "unittest", "-v", testpattern],
            env_map=self._env)
