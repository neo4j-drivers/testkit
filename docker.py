import subprocess

class Container:
    def __init__(self, name):
        self.name = name

    def exec(self, command):
        cmd  = ["docker", "exec", self.name]
        cmd .extend(command)
        subprocess.run(cmd , check=True)

    def exec_detached(self, command):
        cmd  = ["docker", "exec", "--detach", self.name]
        cmd .extend(command)
        subprocess.run(cmd , check=True)

def run(image, name, command=None, mountMap={}, hostMap={}, portMap={}, envMap={}, workingFolder=None, network=None):
    # Bootstrap the driver docker image by running a bootstrap script in the image.
    # The driver docker image only contains the tools needed to build, not the built driver.
    cmd = [ "docker", "run", "--name", name, "--rm", "--detach"]
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
    cmd.append(image)
    if command:
        cmd.extend(command)
    subprocess.run(cmd, check=True)

    return Container(name)
