import docker
import time
import atexit
from os.path import join, abspath


class Server:
    """ Single instance Neo4j server
    """

    def __init__(self, image, name, artifacts_path):
        self.name = name
        self._image = image
        self._artifacts_path = join(artifacts_path, name)
        self._container = None

    def start(self, hostname, port, username, password, edition):
        # Environment variables passed to the Neo4j docker container
        envMap = {
            "NEO4J_dbms_connector_bolt_advertised__address":
                "%s:%d" % (hostname, port),
            "NEO4J_AUTH":
                "%s/%s" % (username, password),
        }
        if edition != "community":
            envMap["NEO4J_ACCEPT_LICENSE_AGREEMENT"] = "yes"
        logs_path = join(self._artifacts_path, "logs")
        self._container = docker.run(
            self._image, hostname,
            mountMap={logs_path: "/logs"},
            envMap=envMap,
            network="the-bridge")

    def stop(self):
        self._container.rm()


class Cluster:
    """ Cluster of Neo4j servers
    """

    def __init__(self, image, name, artifacts_path):
        self.name = name
        self._image = image
        self._artifacts_path = join(artifacts_path, name)

    def start(self, num_cores, num_repl):
        cores = []
        for i in range(num_cores):
            core = Core(i, self._artifacts_path)
            cores.append(core)

        initial_members = ",".join([c.discover for c in cores])

        for core in cores:
            core.start(self._image, initial_members, "the-bridge")


class Core:
    """ Core member of Neo4j cluster
    """

    DISCOVERY_PORT = 5000
    TRANSACTION_PORT = 6000
    RAFT_PORT = 7000

    def __init__(self, index, artifacts_path):
        self.name = "core%d" % index
        self.discover = "%s:%d" % (self.name, Core.DISCOVERY_PORT + index)
        self.transaction = "%s:%d" % (self.name, Core.TRANSACTION_PORT + index)
        self.raft = "%s:%d" % (self.name, Core.RAFT_PORT + index)
        self._index = index
        self._artifacts_path = join(artifacts_path, self.name)
        self._container = None

    def start(self, image, initial_members, network):
        envMap = {
            "NEO4J_dbms_mode":
                "CORE",
            "NEO4J_causal__clustering_discovery__type":
                "LIST",
            "NEO4J_causal__clustering_initial__discovery__members":
                initial_members,
            "NEO4J_causal__clustering_discovery__advertised__address":
                self.discover,
            "NEO4J_causal__clustering_raft__advertised__address":
                self.raft,
            "NEO4J_causal__clustering_transaction__advertised__address":
                self.transaction,
            "NEO4J_causal__clustering_discovery__listen__address":
                "0.0.0.0:%d" % (Core.DISCOVERY_PORT + self._index),
            "NEO4J_causal__clustering_raft__listen__address":
                "0.0.0.0:%d" % (Core.RAFT_PORT + self._index),
            "NEO4J_causal__clustering_transaction__listen__address":
                "0.0.0.0:%d" % (Core.TRANSACTION_PORT + self._index),
            "NEO4J_ACCEPT_LICENSE_AGREEMENT": "yes",
        }
        logs_path = join(self._artifacts_path, "logs")
        self._container = docker.run(image, self.name,
                                     envMap=envMap, network=network,
                                     mountMap={logs_path: "/logs"})


if __name__ == "__main__":
    atexit.register(docker.cleanup)
    cluster = Cluster(
            "neo4j:4.1-enterprise", "cluster", abspath("./artifacts/neo4j"))
    cluster.start(3, 0)
    time.sleep(10)
