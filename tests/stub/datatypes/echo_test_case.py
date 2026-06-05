import abc
from contextlib import contextmanager

from nutkit import protocol as types
from nutkit.frontend import Driver
from tests.shared import TestkitTestCase


class EchoTestCase(TestkitTestCase, abc.ABC):
    @contextmanager
    def _started_server(self, server, script, vars_=None):
        version_folder = "v{}".format(self._bolt_version().replace(".", "x"))
        server.start(
            path=self.script_path(version_folder, script),
            vars_=vars_,
        )
        try:
            yield
        finally:
            server.reset()

    def _driver(self, server):
        uri = "bolt://%s" % server.address
        auth = types.AuthorizationToken("basic", principal="", credentials="")
        return Driver(self._backend, uri, auth)

    @abc.abstractmethod
    def _bolt_version(self):
        ...

    def _test_echo(self, server, jolt_value, cypher_value):
        script = "echo_value.script"
        with self._started_server(
            server,
            script,
            vars_={"#VALUE#": jolt_value},
        ):
            with self._driver(server) as driver:
                with driver.session("r") as session:
                    result = session.run(
                        "RETURN $value AS value",
                        params={"value": cypher_value},
                    )
                    records = list(result)
                    server.done()
                    self.assertEqual(len(records), 1)
                    fields = records[0].values
                    self.assertEqual(len(fields), 1)
                    self.assertEqual(cypher_value, fields[0])
