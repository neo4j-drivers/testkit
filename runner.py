import os

import docker


def _ensure_image(testkit_path, branch_name, artifacts_path):
    """Ensure that an up to date Docker image exists."""
    # Construct Docker image name from branch name
    image_name = "runner:%s" % branch_name
    image_path = os.path.join(testkit_path, "runner_image")
    docker.build_and_tag(image_name, image_path, log_path=artifacts_path)

    return image_name


def start_container(testkit_path, branch_name, network, secondary_network,
                    docker_artifacts_path, build_artifacts_path):
    image = _ensure_image(testkit_path, branch_name, docker_artifacts_path)
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
    for var_name in os.environ:
        if (
            var_name.startswith("TEST_")
            or var_name.startswith("TEAMCITY_")
        ):
            env[var_name] = os.environ[var_name]
    docker.create_or_replace(
        image, "runner",
        command=["python3", "/testkit/driver/bootstrap.py"],
        mount_map={testkit_path: "/testkit"},
        env_map=env,
        network=network,
        aliases=["thehost", "thehostbutwrong"]  # Used when testing TLS
    )
    docker.network_connect(secondary_network, container_name)
    container = docker.start(container_name)
    return Container(container, env, build_artifacts_path)


class Container:
    def __init__(self, container, env, build_artifacts_path):
        self._container = container
        self._env = env
        self._build_artifacts_path = build_artifacts_path
        self._init_container()

    def _init_container(self):
        self._container.exec(["pip3", "install", "-U", "pip"],
                             log_path=self._build_artifacts_path)
        self._container.exec(["pip3", "install", "-Ur",
                              "/testkit/requirements.txt"],
                             log_path=self._build_artifacts_path)

    def run_stub_tests(self):
        self._container.exec(["python3", "-m", "tests.stub.suites"])

    def run_tls_tests(self):
        # Build TLS server
        self._container.exec(
            ["go", "build", "-v", "."], workdir="/testkit/tlsserver"
        )
        self._container.exec(
            ["python3", "-m", "tests.tls.suites"]
        )

    def run_neo4j_tests(self, suite, hostname, username, password,
                        neo4j_config):
        self._env.update({
            # Hostname of Docker container running db
            "TEST_NEO4J_HOST": hostname,
            "TEST_NEO4J_USER": username,
            "TEST_NEO4J_PASS": password,
            "TEST_NEO4J_SCHEME": neo4j_config.scheme,
            "TEST_NEO4J_VERSION": neo4j_config.version,
            "TEST_NEO4J_EDITION": neo4j_config.edition,
            "TEST_NEO4J_CLUSTER": neo4j_config.cluster
        })
        self._container.exec(
            ["python3", "-m", "tests.neo4j.suites", suite, neo4j_config.name],
            env_map=self._env
        )

    def run_neo4j_tests_env_config(self):
        for key in ("TEST_NEO4J_HOST",
                    "TEST_NEO4J_USER",
                    "TEST_NEO4J_PASS",
                    "TEST_NEO4J_SCHEME",
                    "TEST_NEO4J_VERSION",
                    "TEST_NEO4J_EDITION",
                    "TEST_NEO4J_CLUSTER"):
            self._env.update({key: os.environ.get(key)})
        if self._env.get("TEST_NEO4J_HOST") == "localhost":
            self._env.update({"TEST_NEO4J_HOST": "host.docker.internal"})
        suite = os.environ.get("TEST_NEO4J_VERSION", "4.4")
        self._container.exec(
            [
                "python3", "-m", "tests.neo4j.suites", suite,
                f"external-{suite}"
            ],
            env_map=self._env
        )

    def run_selected_stub_tests(self, testpattern):
        self._container.exec(["python3", "-m", "unittest", "-v", testpattern])

    def run_selected_tls_tests(self, testpattern):
        # Build TLS server
        self._container.exec(
            ["go", "build", "-v", "."], workdir="/testkit/tlsserver"
        )
        self._container.exec(["python3", "-m", "unittest", "-v", testpattern])

    def run_selected_neo4j_tests(self, test_pattern, hostname, username,
                                 password, neo4j_config):
        self._env.update({
            # Hostname of Docker container running db
            "TEST_NEO4J_HOST": hostname,
            "TEST_NEO4J_USER": username,
            "TEST_NEO4J_PASS": password,
            "TEST_NEO4J_SCHEME": neo4j_config.scheme,
            "TEST_NEO4J_VERSION": neo4j_config.version,
            "TEST_NEO4J_EDITION": neo4j_config.edition,
            "TEST_NEO4J_CLUSTER": neo4j_config.cluster
        })
        self._container.exec(
            ["python3", "-m", "unittest", "-v", test_pattern],
            env_map=self._env
        )

    def run_selected_neo4j_tests_env_config(self, test_pattern):
        for key in ("TEST_NEO4J_HOST",
                    "TEST_NEO4J_USER",
                    "TEST_NEO4J_PASS",
                    "TEST_NEO4J_SCHEME",
                    "TEST_NEO4J_VERSION",
                    "TEST_NEO4J_EDITION",
                    "TEST_NEO4J_CLUSTER"):
            self._env.update({key: os.environ.get(key)})
        if self._env.get("TEST_NEO4J_HOST") == "localhost":
            self._env.update({"TEST_NEO4J_HOST": "host.docker.internal"})
        self._container.exec(
            ["python3", "-m", "unittest", "-v", test_pattern],
            env_map=self._env
        )
