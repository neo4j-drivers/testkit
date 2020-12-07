import subprocess

_running = {}


class Container:
    def __init__(self, name):
        self.name = name

    def _add(self, cmd, workdir, envMap):
        if workdir:
            cmd.extend(["-w", workdir])
        for k in envMap:
            cmd.extend(["-e", "%s=%s" % (k, envMap[k])])

    def exec(self, command, workdir=None, envMap={}):
        cmd = ["docker", "exec"]
        self._add(cmd, workdir, envMap)
        cmd.append(self.name)
        cmd.extend(command)
        subprocess.run(cmd, check=True)

    def exec_detached(self, command, workdir=None, envMap={}):
        cmd = ["docker", "exec", "--detach"]
        self._add(cmd, workdir, envMap)
        cmd.append(self.name)
        cmd.extend(command)
        subprocess.run(cmd, check=True)

    def rm(self):
        cmd = ["docker", "rm", "-f", "-v", self.name]
        subprocess.run(cmd, check=False,
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        del _running[self.name]


def run(image, name, command=None, mountMap={}, hostMap={}, portMap={},
        envMap={}, workingFolder=None, network=None, aliases=[]):
    # Bootstrap the driver docker image by running a bootstrap script in
    # the image. The driver docker image only contains the tools needed to
    # build, not the built driver.
    cmd = ["docker", "run", "--name", name, "--rm", "--detach"]
    for k in mountMap:
        cmd.extend(["-v", "%s:%s" % (k, mountMap[k])])
    for k in hostMap:
        cmd.extend(["--add-host", "%s:%s" % (k, hostMap[k])])
    for k in portMap:
        cmd.append("-p%d:%d" % (k, portMap[k]))
    for k in envMap:
        cmd.extend(["--env", "%s=%s" % (k, envMap[k])])
    if network:
        cmd.append("--net=%s" % network)
    if workingFolder:
        cmd.extend(["-w", workingFolder])
    for a in aliases:
        cmd.append("--network-alias="+a)
    cmd.append(image)
    if command:
        cmd.extend(command)
    subprocess.run(cmd, check=True)
    container = Container(name)
    _running[name] = container
    return container


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


def cleanup():
    for c in list(_running.values()):
        c.rm()
