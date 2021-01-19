import os
import docker


def start_container(image, testkitPath, driverRepoPath, artifactsPath,
                    gluePath):
    # Bootstrap the driver docker image by running a bootstrap script in
    # the image. The driver docker image only contains the tools needed to
    # build, not the built driver.
    container = docker.run(
        image, "driver",
        command=["python3", "/testkit/driver/bootstrap.py"],
        mountMap={
            testkitPath: "/testkit",
            driverRepoPath: "/driver",
            artifactsPath: "/artifacts"
        },
        portMap={9876: 9876},  # For convenience when debugging
        network="the-bridge",
        workingFolder="/driver")
    return Container(container, gluePath)


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

    def _native_env(self, hostname, port, username, password, config):
        env = self._default_env()
        env.update({
            "TEST_NEO4J_HOST":       hostname,
            "TEST_NEO4J_PORT":       port,
            "TEST_NEO4J_USER":       username,
            "TEST_NEO4J_PASS":       password,
            "TEST_NEO4J_SCHEME":     config["scheme"],
            "TEST_NEO4J_EDITION":    config["edition"],
            "TEST_NEO4J_VERSION":    config["version"],
        })
        if config['cluster']:
            env["TEST_NEO4J_IS_CLUSTER"] = "1"

        # To support the legacy .net integration tests
        # TODO: Move this to testkit/driver/dotnet/*.py
        ctrlArgs = ""
        if config["edition"] == "enterprise":
            ctrlArgs += "-e "
        ctrlArgs += config["version"]
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

    def run_stress_tests(self, hostname, port, username, password, config):
        env = self._native_env(hostname, port, username, password, config)
        self._container.exec([
            "python3", self._gluePath + "stress.py"],
            envMap=env)

    def run_integration_tests(self, hostname, port, username, password,
                              config):
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
