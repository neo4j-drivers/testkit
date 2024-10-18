import os
import pathlib
import re
import subprocess
from threading import Thread

_running = {}
_created_tags = set()


def _docker_path(path):
    if isinstance(path, str):
        path = pathlib.Path(path)
    path = path.absolute()
    if path.drive:
        return str(pathlib.PurePosixPath("/" + path.drive[:-1],
                                         *path.parts[1:]))
    return str(path.as_posix())


def _subprocess_run(cmd, *args, log_path=None, background=False, **kwargs):
    class Runner:
        def __init__(self):
            if log_path and ("stdout" in kwargs or "stderr" in kwargs):
                raise ValueError("Cannot specify log_path with "
                                 "stdout or stderr")
            kwargs.setdefault("stdout")
            kwargs.setdefault("stderr")
            self.stopping = False

        def _process_run(self, *args_, **kwargs_):
            try:
                subprocess.run(cmd, *args_, **kwargs_)
            except subprocess.CalledProcessError:
                # ignore when shutting down anyway
                if not self.stopping:
                    raise

        def run(self):
            if not log_path:
                print(cmd)
                self._process_run(*args, **kwargs)
            else:
                out_path = os.path.join(log_path, "out.log")
                err_path = os.path.join(log_path, "err.log")
                with open(out_path, "a") as out_fd:
                    with open(err_path, "a") as err_fd:
                        kwargs.update(stdout=out_fd, stderr=err_fd)
                        out_fd.write(str(cmd) + "\n")
                        out_fd.flush()
                        err_fd.write(str(cmd) + "\n")
                        err_fd.flush()
                        kwargs_ = {**kwargs, "stdout": out_fd,
                                   "stderr": err_fd}
                        print(cmd)
                        try:
                            self._process_run(*args, **kwargs_)
                        finally:
                            out_fd.write("\n")
                            out_fd.flush()
                            err_fd.write("\n")
                            err_fd.flush()

    runner = Runner()
    if not background:
        runner.run()
    else:
        Thread(target=runner.run, daemon=True).start()
    return runner


class Container:
    def __init__(self, name, runners=None):
        self.name = name
        if runners is None:
            runners = []
        self.runners = runners

    def _add(self, cmd, workdir, env_map):
        if workdir:
            cmd.extend(["-w", workdir])
        if env_map is not None:
            for k in env_map:
                cmd.extend(["-e", "%s=%s" % (k, env_map[k])])

    def _exec(self, command, workdir=None, env_map=None, log_path=None,
              background=False):
        cmd = ["docker", "exec"]
        self._add(cmd, workdir, env_map)
        cmd.append(self.name)
        cmd.extend(command)
        self.runners.append(
            _subprocess_run(
                cmd, log_path=log_path, background=background, check=True
            )
        )

    def exec(self, command, workdir=None, env_map=None, log_path=None):
        self._exec(command, workdir=workdir, env_map=env_map,
                   log_path=log_path)

    def exec_background(self, command, workdir=None, env_map=None,
                        log_path=None):
        self._exec(command, workdir=workdir, env_map=env_map,
                   log_path=log_path, background=True)

    def rm(self):
        for runner in self.runners:
            runner.stopping = True
        cmd = ["docker", "rm", "-f", "-v", self.name]
        print(cmd)
        subprocess.run(cmd, check=False,
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        del _running[self.name]


def create_or_replace(image, name, command=None, mount_map=None, host_map=None,
                      port_map=None, env_map=None, working_folder=None,
                      network=None, aliases=None):
    if name in _running:
        _running[name].rm()
    else:
        subprocess.run(["docker", "rm", "-fv", name], check=True)
    cmd = ["docker", "create", "--name", name]
    if mount_map is not None:
        for k in mount_map:
            src, dst = _docker_path(k), mount_map[k]
            cmd.extend(["-v", "%s:%s" % (src, dst)])
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
    print(cmd)
    subprocess.run(cmd, check=True)


def start(name):
    cmd = ["docker", "start", name]
    print(cmd)
    runner = _subprocess_run(cmd, check=True)
    container = Container(name, runners=[runner])
    _running[name] = container
    return container


def run(image, name, command=None, mount_map=None, host_map=None,
        port_map=None, env_map=None, working_folder=None, network=None,
        aliases=None, log_path=None, background=False):
    # Bootstrap the driver docker image by running a bootstrap script in
    # the image. The driver docker image only contains the tools needed to
    # build, not the built driver.
    cmd = ["docker", "run", "--name", name, "--rm"]
    if not background:
        cmd.append("--detach")
    if mount_map is not None:
        for k in mount_map:
            cmd.extend(["-v", "%s:%s" % (_docker_path(k), mount_map[k])])
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
    runner = _subprocess_run(cmd, check=True, log_path=log_path,
                             background=background)
    container = Container(name, runners=[runner])
    _running[name] = container
    return container


def network_connect(network, name):
    cmd = ["docker", "network", "connect", network, name]
    print(cmd)
    subprocess.run(cmd, check=True)


def load(readable):
    cmd = ["docker", "load"]
    p = subprocess.Popen(cmd, stdin=subprocess.PIPE,
                         stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE)
    outs, errs = p.communicate(input=readable.read())
    loaded_image = None
    if outs:
        outs = outs.decode("utf-8")
        print(outs)
        for line in outs.split("\n"):
            if line.lower().strip().startswith("loaded image: "):
                if loaded_image is not None:
                    raise ValueError("Loaded multiple images:\n%s" % outs)
                loaded_image = line.rsplit(maxsplit=1)[1]
    if errs:
        print(str(errs))
    if p.returncode != 0:
        raise Exception("Failed to load docker image")
    if loaded_image is None:
        raise Exception("Docker reported no loaded image:\n%s" % outs)
    return loaded_image


def build_and_tag(tag_name, dockerfile_path, cwd=None,
                  log_path=None, args=None):
    if args is None:
        args = {}
    build_args = [e for k, v in args.items()
                  for e in ("--build-arg", f"{k}={v}")]

    cmd = ["docker", "build", *build_args, "--tag", tag_name, dockerfile_path]
    print(cmd)

    if not log_path:
        subprocess.check_call(cmd, cwd=cwd)
    else:
        clean_tag = re.sub(r"\W", "_", tag_name)
        out_path = os.path.join(log_path, "build_{}_out.log".format(clean_tag))
        err_path = os.path.join(log_path, "build_{}_err.log".format(clean_tag))
        with open(out_path, "w") as out_fd:
            with open(err_path, "w") as err_fd:
                subprocess.check_call(cmd,
                                      cwd=cwd, stdout=out_fd, stderr=err_fd)
    _created_tags.add(tag_name)
    remove_dangling()


def cleanup(settings):
    for c in list(_running.values()):
        c.rm()
    if settings.docker_rmi:
        for t in _created_tags:
            print("cleanup (docker rmi %s)" % t)
            subprocess.run(["docker", "rmi", t])


def remove_dangling():
    print("Checking for dangling intermediate images")
    images = subprocess.check_output([
        "docker", "images", "-a", "--filter=dangling=true", "-q"
    ], encoding="utf-8").splitlines()
    if len(images):
        print("Cleaning up dangling images (docker rmi %s)" % " ".join(images))
        # Sometimes fails, do not fail build due to that
        subprocess.run(["docker", "rmi", " ".join(images)])
