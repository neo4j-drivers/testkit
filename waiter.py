import os

import docker


def _ensure_image(testkit_path, branch_name, artifacts_path):
    """Ensure that an up to date Docker image exists."""
    # Construct Docker image name from branch name
    image_name = "waiter:%s" % branch_name
    image_path = os.path.join(testkit_path, "waiter")
    docker.build_and_tag(image_name, image_path, log_path=artifacts_path)

    return image_name


def start_container(testkit_path, branch_name, network, docker_artifacts_path,
                    build_artifacts_path, artifacts_path):
    image = _ensure_image(testkit_path, branch_name, docker_artifacts_path)
    container_name = "waiter"
    docker.create_or_replace(image, container_name, network=network)
    container = docker.start(container_name)
    return Container(container, build_artifacts_path, artifacts_path)


class Container:
    def __init__(self, container, build_artifacts_path, artifacts_path):
        self._container = container
        self._build_artifacts_path = build_artifacts_path
        self._artifacts_path = artifacts_path
        self._init_container()

    def _init_container(self):
        self._container.exec(
            ["python3", "-m", "venv", "venv"],
            log_path=self._build_artifacts_path
        )
        self._container.exec(
            ["venv/bin/pip", "install", "-U", "pip"],
            log_path=self._build_artifacts_path
        )
        self._container.exec(
            ["venv/bin/pip", "install", "-Ur", "requirements.txt"],
            log_path=self._build_artifacts_path
        )

    def wait_for_all_dbs(self, hostname, port, user, password):
        self._container.exec(
            [
                "venv/bin/python", "wait_for_all_dbs.py",
                hostname, "%d" % port, user, password,
            ],
            log_path=self._artifacts_path,
        )
