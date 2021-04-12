import os
import subprocess


_running = {}
_created_tags = set()


class Container:
    def __init__(self, name):
        self.name = name

    def _add(self, cmd, workdir, env_map):
        if workdir:
            cmd.extend(["-w", workdir])
        if env_map is not None:
            for k in env_map:
                cmd.extend(["-e", "%s=%s" % (k, env_map[k])])

    def exec(self, command, workdir=None, env_map=None):
        cmd = ["docker", "exec"]
        self._add(cmd, workdir, env_map)
        cmd.append(self.name)
        cmd.extend(command)
        subprocess.run(cmd, check=True)

    def exec_detached(self, command, workdir=None, env_map=None):
        cmd = ["docker", "exec", "--detach"]
        self._add(cmd, workdir, env_map)
        cmd.append(self.name)
        cmd.extend(command)
        subprocess.run(cmd, check=True)

    def rm(self):
        cmd = ["docker", "rm", "-f", "-v", self.name]
        print('docker rm -f -v "%s"' % self.name)
        subprocess.run(cmd, check=False,
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        del _running[self.name]


def create(image, name, command=None, mount_map=None, host_map=None, port_map=None,
           env_map=None, working_folder=None, network=None, aliases=None):
    cmd = ["docker", "create", "--name", name]
    if mount_map is not None:
        for k in mount_map:
            cmd.extend(["-v", "%s:%s" % (k, mount_map[k])])
    if host_map is not None:
        for k in host_map:
            cmd.extend(["--add-host", "%s:%s" % (k, host_map[k])])
    if port_map is not None:
        for k in port_map:
            cmd.append("-p%d:%d" % (k, port_map[k]))
    if env_map is not None:
        for k in env_map:
            cmd.extend(["--env", "%s=%s" % (k, env_map[k])])
    if network:
        cmd.append("--net=%s" % network)
    if working_folder:
        cmd.extend(["-w", working_folder])
    if aliases is not None:
        for a in aliases:
            cmd.append("--network-alias=" + a)
    if "TEST_DOCKER_USER" in os.environ:
        cmd.extend(["-u", os.environ["TEST_DOCKER_USER"]])
    cmd.append(image)
    if command:
        cmd.extend(command)
    subprocess.run(cmd, check=True)


def start(name):
    cmd = ["docker", "start", name]
    subprocess.run(cmd, check=True)
    container = Container(name)
    _running[name] = container
    return container


def run(image, name, command=None, mount_map=None, host_map=None, port_map=None,
        env_map=None, working_folder=None, network=None, aliases=None):
    # Bootstrap the driver docker image by running a bootstrap script in
    # the image. The driver docker image only contains the tools needed to
    # build, not the built driver.
    cmd = ["docker", "run", "--name", name, "--rm", "--detach"]
    if mount_map is not None:
        for k in mount_map:
            cmd.extend(["-v", "%s:%s" % (k, mount_map[k])])
    if host_map is not None:
        for k in host_map:
            cmd.extend(["--add-host", "%s:%s" % (k, host_map[k])])
    if port_map is not None:
        for k in port_map:
            cmd.append("-p%d:%d" % (k, port_map[k]))
    if env_map is not None:
        for k in env_map:
            cmd.extend(["--env", "%s=%s" % (k, env_map[k])])
    if network:
        cmd.append("--net=%s" % network)
    if working_folder:
        cmd.extend(["-w", working_folder])
    if aliases is not None:
        for a in aliases:
            cmd.append("--network-alias="+a)
    if "TEST_DOCKER_USER" in os.environ:
        cmd.extend(["-u", os.environ["TEST_DOCKER_USER"]])
    cmd.append(image)
    if command:
        cmd.extend(command)
    subprocess.run(cmd, check=True)
    container = Container(name)
    _running[name] = container
    return container


def network_connect(network, name):
    cmd = ["docker", "network", "connect", network, name]
    subprocess.run(cmd, check=True)


def load(readable):
    cmd = ["docker", "load"]
    p = subprocess.Popen(cmd, stdin=subprocess.PIPE)
    outs, errs = p.communicate(input=readable.read())
    if outs:
        print(str(outs))
    if errs:
        print(str(errs))
    if p.returncode != 0:
        raise Exception("Failed to load docker image")


def build_and_tag(tag_name, dockerfile_path, cwd=None):
    print("Building runner Docker image %s from %s" % (tag_name,
                                                       dockerfile_path))
    subprocess.check_call([
        "docker", "build", "--tag", tag_name, dockerfile_path
    ], cwd=cwd)
    _created_tags.add(tag_name)
    remove_dangling()


def cleanup():
    for c in list(_running.values()):
        c.rm()
    if os.environ.get("TEST_DOCKER_RMI", "").lower() \
            in ("true", "y", "yes", "1", "on"):
        for t in _created_tags:
            print('docker rmi "%s"' % t)
            subprocess.run(["docker", "rmi", t])


def remove_dangling():
    print("Checking for dangling intermediate images")
    images = subprocess.check_output([
        "docker", "images", "-a", "--filter=dangling=true", "-q"
    ], encoding="utf-8").splitlines()
    if len(images):
        print("Cleaning up images: %s" % images)
        # Sometimes fails, do not fail build due to that
        subprocess.run(["docker", "rmi", " ".join(images)])
