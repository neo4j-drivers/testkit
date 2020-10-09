"""
Shared utilities for writing stub tests

Uses environment variables for configuration:
"""
import subprocess, os, time, io, platform, tempfile


class StubServer:
    def __init__(self, port):
        self.host = os.environ.get("TEST_STUB_HOST", "127.0.0.1")
        self.address = "%s:%d" % (self.host, port)
        self.port = port
        self._process = None

    def start(self, path=None, script=None, vars={}):
        if self._process:
            raise Exception("Stub server in use")

        if platform.system() == "Windows":
            pythonCommand = "python"
        else:
            pythonCommand = "python3"

        if script:
            tempdir = tempfile.gettempdir()
            path = os.path.join(tempdir, "temp.script")
            # print("Generating script file in %s" % path)
            for v in vars:
                script = script.replace(v, str(vars[v]))
            with open(path, "w") as f:
                f.write(script)

        if path:
            print("Starting stubserver on %s with script %s" % (self.address, path))
            self._process = subprocess.Popen([pythonCommand,
                                              "-m",
                                              "boltstub",
                                              "-l",
                                              "0.0.0.0:%d" % self.port,
                                              "-v",
                                              path],
                                             stdout=subprocess.PIPE,
                                             stderr=subprocess.PIPE,
                                             close_fds=True,
                                             encoding='utf-8')

        # Wait until something is written to know it started, requires -v
        self._process.stdout.readline()

        # Double check that the process started, a missing script would exit process immediately
        if self._process.poll():
            self._dump()
            self._process = None
            raise Exception("Stub server didn't start")


    def _dump(self):
        # print("")
        print(">>>> Captured stub server %s stdout" % self.address)
        for line in self._process.stdout:
            print(line)
        print("<<<< Captured stub server %s stdout" % self.address)

        print(">>>> Captured stub server %s stderr" % self.address)
        for line in self._process.stderr:
            print(line)
        print("<<<< Captured stub server %s stderr" % self.address)

        self._close_pipes()

    def _close_pipes(self):
        self._process.stdout.close()
        self._process.stderr.close()

    def _kill(self):
        self._process.kill()
        self._process.wait()
        self._dump()
        self._process = None

    def done(self):
        """ Checks if the server stopped nicely (processes exited with 0), if so this method is done.
        If the server process exited with non 0, an exception will be raised.
        If the server process is running it will be polled until timeout and proceed as above.
        If the server process is still running after polling it will be killed and an exception will be raised.
        """
        polls = 100
        while polls:
            self._process.poll()
            if self._process.returncode is None:
                time.sleep(0.1)
                polls -= 1
            else:
                if self._process.returncode:
                    self._dump()
                    self._process = None
                    raise Exception("Stub server exited unclean")
                self._close_pipes()
                self._process = None
                return

        self._kill()
        raise Exception("Stub server hanged")

    def reset(self):
        if self._process:
            self._kill()

scripts_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "scripts")
