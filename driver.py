import os
import shutil
import subprocess

import docker
import neo4j


def _get_glue(thisPath, driverName, driverRepo):
    """ Locates where driver has it's docker image and Python "glue" scripts
    needed to build and run tests for the driver.
    Returns a tuple consisting of the absolute path on this machine along with
    the path as it will be mounted in the driver container (need trailing
    slash).
    """
    in_driver_repo = os.path.join(driverRepo, "testkit")
    if os.path.isdir(in_driver_repo):
        return (in_driver_repo, "/driver/testkit/")

    in_this_repo = os.path.join(thisPath, "driver", driverName)
    if os.path.isdir(in_this_repo):
        return (in_this_repo, "/testkit/driver/%s/" % driverName)

    raise Exception("No glue found for %s" % driverName)


def _ensure_image(testkit_path, docker_image_path, branch_name, driver_name):
    """ Ensures that an up to date Docker image exists for the driver.
    """
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
                                   "certs", "driver")
    shutil.copytree(cas_source_path, cas_path)

    # This will use the driver folder as build context.
    print("Building driver Docker image %s from %s"
          % (image_name, docker_image_path))
    subprocess.check_call([
        "docker", "build", "--tag", image_name, docker_image_path])

    docker.remove_dangling()

    return image_name


def start_container(testkit_path, branch_name, driver_name, driver_path,
                    artifacts_path, network=None):
    # Path where scripts are that adapts driver to testkit.
    # Both absolute path and path relative to driver container.
    host_glue_path, driver_glue_path = _get_glue(testkit_path, driver_name,
                                                 driver_path)
    image = _ensure_image(testkit_path, host_glue_path,
                          branch_name, driver_name)
    # Configure volume map for the driver container
    mountMap = {
        testkit_path:   "/testkit",
        driver_path:    "/driver",
        artifacts_path: "/artifacts"
    }
    if os.environ.get("TEST_BUILD_CACHE_ENABLED") == "true":
        if driver_name == "java":
            mountMap["testkit-m2"] = "/root/.m2"
    # Bootstrap the driver docker image by running a bootstrap script in
    # the image. The driver docker image only contains the tools needed to
    # build, not the built driver.
    container = docker.run(
        image, "driver",
        command=["python3", "/testkit/driver/bootstrap.py"],
        mountMap=mountMap,
        portMap={9876: 9876},  # For convenience when debugging
        network=network,
        workingFolder="/driver")
    return Container(container, driver_glue_path)


class Container:
    """ Represents the driver running in a Docker container.
    """

    def __init__(self, container, gluePath):
        self._container = container
        self._gluePath = gluePath

    def _default_env(self):
        env = {}
        # Copy TEST_ variables that might have been set explicit
        for varName in os.environ:
            if varName.startswith("TEST_"):
                env[varName] = os.environ[varName]
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
        ctrlArgs = ""
        if config.edition == "enterprise":
            ctrlArgs += "-e "
        ctrlArgs += config.version
        env["NEOCTRL_ARGS"] = ctrlArgs

        return env

    def clean_artifacts(self):
        """ We let the driver container clean the artifacts due to problem
        with removing files created by containers from host. If we fix the
        user accounts used for the images this can be done from host
        instead.
        """
        self._container.exec(
                ["python3", "/testkit/driver/clean_artifacts.py"],
                envMap=self._default_env())

    def build_driver_and_backend(self):
        self._container.exec(
                ["python3", self._gluePath + "build.py"],
                envMap=self._default_env())

    def run_unit_tests(self):
        self._container.exec(
                ["python3", self._gluePath + "unittests.py"],
                envMap=self._default_env())

    def run_stress_tests(self, hostname, port, username, password,
                         config: neo4j.Config) -> None:
        env = self._native_env(hostname, port, username, password, config)
        self._container.exec([
            "python3", self._gluePath + "stress.py"],
            envMap=env)

    def run_integration_tests(self, hostname, port, username, password,
                              config: neo4j.Config):
        env = self._native_env(hostname, port, username, password, config)
        self._container.exec([
            "python3", self._gluePath + "integration.py"],
            envMap=env)

    def start_backend(self):
        env = self._default_env()
        # Note that this is done detached which means that we don't know for
        # sure if the test backend actually started and we will not see
        # any output of this command.
        # When failing due to not being able to connect from client or seeing
        # issues like 'detected possible backend crash', make sure that this
        # works simply by commenting detach and see that the backend starts.
        self._container.exec_detached(
                ["python3", self._gluePath + "backend.py"],
                envMap=env)
        # Wait until backend started
        # Use driver container to check for backend availability
        self._container.exec([
            "python3",
            "/testkit/driver/wait_for_port.py", "localhost", "%d" % 9876],
            envMap=env)

    def poll_host_and_port_until_available(self, hostname, port):
        self._container.exec([
            "python3", "/testkit/driver/wait_for_port.py",
            hostname, "%d" % port])

    def assert_connections_closed(self, hostname, port):
        self._container.exec([
            "python3", "/testkit/driver/assert_conns_closed.py",
            hostname, "%d" % port])
