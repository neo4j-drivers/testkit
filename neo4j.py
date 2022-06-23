"""Neo4j instance test configuration (no runtime properties)."""

from dataclasses import dataclass
import os
from os.path import join
from typing import Optional

import docker
from teamcity.download import DockerImage


@dataclass
class Config:
    """Configuration for a Neo4j instance."""

    name: str
    image: str
    version: str
    edition: str
    cluster: bool
    suite: str
    scheme: str
    download: Optional[DockerImage]
    stress_test_duration: int


username = "neo4j"
password = "pass"


class Standalone:
    """Single instance Neo4j server."""

    def __init__(self, image, name, artifacts_path, hostname, port, edition):
        self.name = name
        self._image = image
        self._artifacts_path = join(artifacts_path, name)
        self._container = None
        self._hostname = hostname
        self._port = port
        self._edition = edition

    def start(self, network):
        # Environment variables passed to the Neo4j docker container
        env_map = {
            "NEO4J_dbms_connector_bolt_advertised__address":
                "%s:%d" % (self._hostname, self._port),
            "NEO4J_AUTH":
                "%s/%s" % (username, password),
        }
        if self._edition != "community":
            env_map["NEO4J_ACCEPT_LICENSE_AGREEMENT"] = "yes"
        logs_path = join(self._artifacts_path, "logs")
        self._container = docker.run(
            self._image, self._hostname,
            mount_map={logs_path: "/logs"},
            env_map=env_map,
            network=network)

    def addresses(self):
        return [(self._hostname, self._port)]

    def stop(self):
        self._container.rm()
        self._container = None


class Cluster:
    """Cluster of Neo4j servers."""

    def __init__(self, image, name, artifacts_path, num_cores=3):
        self.name = name
        self._image = image
        self._artifacts_path = join(artifacts_path, name)
        self._num_cores = num_cores
        self._cores = []

    def start(self, network):
        for i in range(self._num_cores):
            core = Core(i, self._artifacts_path)
            self._cores.append(core)

        initial_members = ",".join([c.discover for c in self._cores])

        for core in self._cores:
            core.start(self._image, initial_members, network)

    def addresses(self):
        return [("core%d" % i, 7687) for i in range(self._num_cores)]

    def stop(self):
        for core in self._cores:
            core.stop()


class Core:
    """Core member of Neo4j cluster."""

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
        env_map = {
            "NEO4J_dbms_connector_bolt_advertised__address":
                "%s:%d" % (self.name, 7687),
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
            "NEO4J_AUTH":
                "%s/%s" % (username, password),
        }
        # Config options renamed in 5.0 (old versions are deprecated and
        # still working; at lest they should be ;) )
        for old_key, new_key in ({
            "NEO4J_causal__clustering_transaction__advertised__address":
                "NEO4J_server_cluster_advertised__address",
            "NEO4J_causal__clustering_transaction__listen__address":
                "NEO4J_server_cluster_listen__address",
            "NEO4J_causal__clustering_raft__advertised__address":
                "NEO4J_server_cluster_raft_advertised__address",
            "NEO4J_causal__clustering_raft__listen__address":
                "NEO4J_server_cluster_raft_listen__address",
            "NEO4J_causal__clustering_discovery__advertised__address":
                "NEO4J_server_discovery_advertised__address",
            "NEO4J_causal__clustering_discovery__listen__address":
                "NEO4J_server_discovery_listen__address",
            "NEO4J_causal__clustering_initial__discovery__members":
                "NEO4J_dbms_cluster_discovery_initial__members",
            "NEO4J_causal__clustering_discovery__type":
                "NEO4J_dbms_cluster_discovery_type",
            "NEO4J_dbms_connector_bolt_advertised__address":
                "NEO4J_server_bolt_advertised__address",
        }).items():
            env_map[new_key] = env_map[old_key]
        logs_path = join(self._artifacts_path, "logs")
        os.makedirs(logs_path, exist_ok=True)

        self._container = docker.run(
            image, self.name,
            env_map=env_map, network=network, mount_map={logs_path: "/logs"},
            log_path=self._artifacts_path, background=True
        )

    def stop(self):
        self._container.rm()
        self._container = None
