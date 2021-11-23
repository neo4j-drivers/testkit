import os
import shutil

import docker
import neo4j


def _get_glue(this_path, driver_name, driver_repo):
    """Locate where driver has it's docker image and Python "glue" scripts.

    The "glue" is  needed to build and run tests for the driver.
    Returns a tuple consisting of the absolute path on this machine along with
    the path as it will be mounted in the driver container (need trailing
    slash).
    """
    in_driver_repo = os.path.join(driver_repo, "testkit")
    if os.path.isdir(in_driver_repo):
        return (in_driver_repo, "/driver/testkit/")

    in_this_repo = os.path.join(this_path, "driver", driver_name)
    if os.path.isdir(in_this_repo):
        return (in_this_repo, "/testkit/driver/%s/" % driver_name)

    raise Exception("No glue found for %s" % driver_name)


def _ensure_image(testkit_path, docker_image_path, branch_name, driver_name,
                  artifacts_path):
    """Ensure that an up to date Docker image exists for the driver."""
    # Construct Docker image name from driver name (i.e drivers-go) and
    # branch name (i.e 4.2, go-1.14-image)
    image_name = "drivers-%s:%s" % (driver_name, branch_name)
    # Copy CAs that the driver should know of to the Docker build context
    # (first remove any previous...). Each driver container should contain
    # those CAs in such a way that driver language can use them as system
    # CAs without any custom modification of the driver.
    cas_path = os.path.join(docker_image_path, "CAs")
    shutil.rmtree(cas_path, ignore_errors=True)
    cas_source_path = os.path.join(testkit_path, "tests", "tls",
                                   "certs", "driver", "trusted")
    shutil.copytree(cas_source_path, cas_path)
    custom_cas_path = os.path.join(docker_image_path, "CustomCAs")
    shutil.rmtree(custom_cas_path, ignore_errors=True)
    custom_cas_source_path = os.path.join(testkit_path, "tests", "tls",
                                          "certs", "driver", "custom")
    shutil.copytree(custom_cas_source_path, custom_cas_path)

    # This will use the driver folder as build context.
    docker.build_and_tag(image_name, docker_image_path,
                         log_path=artifacts_path)

    return image_name


def start_container(testkit_path, branch_name, driver_name, driver_path,
                    artifacts_path_build, network, secondary_network):
    # Path where scripts are that adapts driver to testkit.
    # Both absolute path and path relative to driver container.
    host_glue_path, driver_glue_path = _get_glue(testkit_path, driver_name,
                                                 driver_path)
    image = _ensure_image(testkit_path, host_glue_path,
                          branch_name, driver_name, artifacts_path_build)
    container_name = "driver"
    # Configure volume map for the driver container
    mount_map = {
        testkit_path: "/testkit",
        driver_path: "/driver"
    }
    if os.environ.get("TEST_BUILD_CACHE_ENABLED") == "true":
        if driver_name == "java":
            mount_map["testkit-m2"] = "/root/.m2"
    # Bootstrap the driver docker image by running a bootstrap script in
    # the image. The driver docker image only contains the tools needed to
    # build, not the built driver.
    docker.create_or_replace(
        image, container_name,
        command=["python3", "/testkit/driver/bootstrap.py"],
        mount_map=mount_map,
        host_map={"host.docker.internal": "host-gateway"},
        port_map={9876: 9876},  # For convenience when debugging
        network=network,
        working_folder="/driver")
    docker.network_connect(secondary_network, container_name)
    container = docker.start(container_name)
    return Container(container, driver_glue_path)


class Container:
    """Represents the driver running in a Docker container."""

    def __init__(self, container, glue_path):
        self._container = container
        self._glue_path = glue_path

    def _default_env(self):
        env = {}
        # Copy TEST_ variables that might have been set explicit
        for var_name in os.environ:
            if var_name.startswith("TEST_"):
                env[var_name] = os.environ[var_name]
        return env

    def _native_env(self, hostname, port, username, password,
                    config: neo4j.Config):
        env = self._default_env()
        env.update({
            "TEST_NEO4J_HOST":    hostname,
            "TEST_NEO4J_PORT":    port,
            "TEST_NEO4J_USER":    username,
            "TEST_NEO4J_PASS":    password,
            "TEST_NEO4J_SCHEME":  config.scheme,
            "TEST_NEO4J_EDITION": config.edition,
            "TEST_NEO4J_VERSION": config.version,
        })
        if config.cluster:
            env["TEST_NEO4J_IS_CLUSTER"] = "1"

        env["TEST_NEO4J_STRESS_DURATION"] = config.stress_test_duration

        # To support the legacy .net integration tests
        # TODO: Move this to testkit/driver/dotnet/*.py
        ctrl_args = ""
        if config.edition == "enterprise":
            ctrl_args += "-e "
        ctrl_args += config.version
        env["NEOCTRL_ARGS"] = ctrl_args

        return env

    def build_driver_and_backend(self, artifacts_path):
        self._container.exec(
            ["python3", self._glue_path + "build.py"],
            env_map=self._default_env(), log_path=artifacts_path

        )

    def run_unit_tests(self):
        self._container.exec(
            ["python3", self._glue_path + "unittests.py"],
            env_map=self._default_env()
        )

    def run_stress_tests(self, hostname, port, username, password,
                         config: neo4j.Config) -> None:
        env = self._native_env(hostname, port, username, password, config)
        self._container.exec([
            "python3", self._glue_path + "stress.py"],
            env_map=env)

    def run_integration_tests(self, hostname, port, username, password,
                              config: neo4j.Config):
        env = self._native_env(hostname, port, username, password, config)
        self._container.exec([
            "python3", self._glue_path + "integration.py"],
            env_map=env)

    def start_backend(self, artifacts_path):
        env = self._default_env()
        # Note that this is done detached which means that we don't know for
        # sure if the test backend actually started and we will not see
        # any output of this command.
        # When failing due to not being able to connect from client or seeing
        # issues like 'detected possible backend crash', make sure that this
        # works simply by commenting detach and see that the backend starts.
        self._container.exec_detached(
            ["python3", self._glue_path + "backend.py"],
            env_map=env, log_path=artifacts_path
        )
        # Wait until backend started
        # Use driver container to check for backend availability
        self._container.exec([
            "python3",
            "/testkit/driver/wait_for_port.py", "localhost", "%d" % 9876],
            env_map=env)

    def poll_host_and_port_until_available(self, hostname, port):
        self._container.exec([
            "python3", "/testkit/driver/wait_for_port.py",
            hostname, "%d" % port])

    def assert_connections_closed(self, hostname, port):
        self._container.exec([
            "python3", "/testkit/driver/assert_conns_closed.py",
            hostname, "%d" % port])
