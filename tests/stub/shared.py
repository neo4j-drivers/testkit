""" Shared utilities for writing stub tests

Uses environment variables for configuration:
"""
import os
import platform
from queue import (
    Empty,
    Queue,
)
import re
import signal
import subprocess
import sys
import tempfile
from threading import Thread
import time


class StubServerUncleanExit(Exception):
    pass


def _poll_pipe(pipe, queue):
    for line in iter(pipe.readline, ""):
        queue.put(line)
    pipe.close()


class StubServer:
    def __init__(self, port):
        self.host = os.environ.get("TEST_STUB_HOST", "127.0.0.1")
        self.address = "%s:%d" % (self.host, port)
        self.port = port
        self._process = None
        self._stdout_buffer = Queue()
        self._stdout_lines = []
        self._stderr_buffer = Queue()
        self._stderr_lines = []
        self._pipes_closed = False

    def start(self, path=None, script=None, vars=None):
        if self._process:
            raise Exception("Stub server in use")

        if platform.system() == "Windows":
            python_command = "python"
        else:
            python_command = "python3"

        if script:
            tempdir = tempfile.gettempdir()
            path = os.path.join(tempdir, "temp.script")
            if vars is not None:
                for v in vars:
                    script = script.replace(v, str(vars[v]))
            with open(path, "w") as f:
                f.write(script)

        self._process = subprocess.Popen([python_command,
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

        Thread(target=_poll_pipe,
               daemon=True,
               args=(self._process.stdout, self._stdout_buffer)).start()
        Thread(target=_poll_pipe,
               daemon=True,
               args=(self._process.stderr, self._stderr_buffer)).start()

        # Wait until something is written to know it started, requires
        polls = 100
        self._read_pipes()
        while (self._process.poll() is None
               and polls
               and "Listening\n" not in self._stdout_lines):
            time.sleep(0.1)
            self._read_pipes()
            polls -= 1

        # Double check that the process started, a missing script would exit
        # process immediately
        if self._process.poll():
            self._dump()
            self._process = None
            raise StubServerUncleanExit("Stub server crashed on start-up")

    def __del__(self):
        if self._process:
            self._process.kill()

    def _read_pipes(self):
        while True:
            try:
                self._stdout_lines.append(self._stdout_buffer.get(False))
            except Empty:
                break
        while True:
            try:
                self._stderr_lines.append(self._stderr_buffer.get(False))
            except Empty:
                break

    def _dump(self):
        self._read_pipes()
        sys.stdout.flush()
        print(">>>> Captured stub server %s stdout" % self.address)
        for line in self._stdout_lines:
            print(line, end="")
        print("<<<< Captured stub server %s stdout" % self.address)

        print(">>>> Captured stub server %s stderr" % self.address)
        for line in self._stderr_lines:
            print(line, end="")
        print("<<<< Captured stub server %s stderr" % self.address)

        # self._close_pipes()
        sys.stdout.flush()

    def _kill(self):
        self._process.kill()
        self._process.wait()
        self._dump()
        self._process = None

    def _interrupt(self, timeout=5.):
        polls = int(timeout * 10)
        self._process.send_signal(signal.SIGINT)
        while polls:
            self._process.poll()
            if self._process.returncode is None:
                time.sleep(0.1)
                polls -= 1
            else:
                return True
        return False

    def done(self):
        # TODO: update doc string
        """ Checks if the server stopped nicely (processes exited with 0),
        if so this method is done.
        If the server process exited with non 0, an exception will be raised.
        If the server process is running it will be polled until timeout and
          proceed as above.
        If the server process is still running after polling it will be killed
          and an exception will be raised.
        """
        if not self._process:
            # test was probably skipped or failed before the stub server could
            # be started.
            return
        try:
            if self._interrupt():
                pass
            elif self._interrupt():
                raise StubServerUncleanExit(
                    "Stub server didn't finish the script."
                )
            elif not self._interrupt():
                self._process.kill()
                self._process.wait()
                raise StubServerUncleanExit("Stub server hanged.")
            if self._process.returncode not in (0, -signal.SIGINT):
                self._dump()
                raise StubServerUncleanExit(
                    "Stub server exited unclean ({})".format(
                        self._process.returncode
                    )
                )
        except Exception:
            self._dump()
            raise
        finally:
            if self._process.returncode not in (0, -signal.SIGINT):
                self._dump()
                raise Exception(
                    "Stub server exited unclean ({})".format(
                        self._process.returncode
                    )
                )
            self._read_pipes()
            self._process = None

    def stop(self):
        if not self._process:
            # test was probably skipped or failed before the stub server could
            # be started.
            return
        running = True
        for _ in range(3):
            if self._interrupt(1):
                running = False
                break
        if running:
            self._process.kill()
            self._process.wait()
            raise StubServerUncleanExit("Stub server hanged.")
        self._read_pipes()
        self._process = None

    def reset(self):
        if self._process:
            # briefly try to get a shutdown that will dump script mismatches
            self._interrupt(0)
            self._interrupt(1)
            self._kill()

    def count_requests_re(self, pattern):
        if isinstance(pattern, re.Pattern):
            return self.count_requests(pattern)
        return self.count_requests(re.compile(pattern))

    def count_requests(self, pattern):
        self._read_pipes()
        count = 0
        for line in self._stdout_lines:
            # lines start with something like "10:08:33  [#2328]  "
            # plus some color escape sequences and ends on a newline
            line = line[30:-1]
            if not line.startswith("C: "):
                continue
            line = line[3:]
            if isinstance(pattern, re.Pattern):
                count += bool(pattern.match(line))
            else:
                count += line.startswith(pattern)
        return count

    def count_responses_re(self, pattern):
        if isinstance(pattern, re.Pattern):
            return self.count_responses(pattern)
        return self.count_responses(re.compile(pattern))

    def count_responses(self, pattern):
        self._read_pipes()
        count = 0
        for line in self._stdout_lines:
            # lines start with something like "10:08:33  [#2328]  "
            # plus some color escape sequences and ends on a newline
            line = line[30:-1]
            match = re.match(r"^(S: )|(\(\d+\)S: )|(\(\d+\)\s+)", line)
            if not match:
                continue
            line = line[match.end():]
            if isinstance(pattern, re.Pattern):
                count += bool(pattern.match(line))
            else:
                count += line.startswith(pattern)
        return count


scripts_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "scripts")
