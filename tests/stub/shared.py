"""Shared utilities for writing stub tests.

Uses environment variables for configuration:
"""

import errno
import os
import platform
import re
import signal
import subprocess
import sys
import tempfile
import time
from queue import (
    Empty,
    Queue,
)
from textwrap import wrap
from threading import Thread

if platform.system() == "Windows":
    INTERRUPT = signal.CTRL_BREAK_EVENT
    INTERRUPT_EXIT_CODE = 3221225786  # oh Windows, you absolute beauty
    POPEN_EXTRA_KWARGS = {"creationflags": subprocess.CREATE_NEW_PROCESS_GROUP}
else:
    INTERRUPT = signal.SIGINT
    INTERRUPT_EXIT_CODE = -signal.SIGINT
    POPEN_EXTRA_KWARGS = {}


class StubServerError(Exception):
    pass


class StubServerUncleanExitError(StubServerError):
    pass


class StubScriptNotFinishedError(StubServerError):
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
        self._script_path = None

    def start(self, path=None, script=None, vars_=None):
        if self._process:
            raise Exception("Stub server in use")

        self._stdout_buffer = Queue()
        self._stdout_lines = []
        self._stderr_buffer = Queue()
        self._stderr_lines = []
        self._pipes_closed = False

        if path and script:
            raise ValueError("Specify either path or script.")

        script_fn = "temp.script"
        if vars_:
            if path:
                script_fn = os.path.basename(path)
                with open(path, "r") as f:
                    script = f.read()
            for v in vars_:
                script = script.replace(v, str(vars_[v]))
        if script:
            tempdir = tempfile.gettempdir()
            path = os.path.join(tempdir, script_fn)
            with open(path, "w") as f:
                f.write(script)
                f.flush()
                os.fsync(f)
            self._script_path = path

        self._process = subprocess.Popen(
            [
                sys.executable, "-m", "boltstub", "-l",
                "0.0.0.0:%d" % self.port, "-v", path
            ],
            **POPEN_EXTRA_KWARGS,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, close_fds=True,
            encoding="utf-8"
        )

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
        try:
            if self._process.poll():
                self._dump()
                self._clean_up()
                raise StubServerUncleanExitError(
                    "Stub server crashed on start-up"
                )
        finally:
            self._rm_tmp_script()

    def __del__(self):
        self._clean_up()

    def _rm_tmp_script(self):
        if self._script_path:
            try:
                os.remove(self._script_path)
            except OSError:
                pass
            self._script_path = None

    def _clean_up(self):
        if self._process:
            self._process.kill()
        self._process = None
        self._rm_tmp_script()

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
        if self._process.returncode > 0:
            self._dump()
        else:
            self._read_pipes()
        self._clean_up()

    def _poll(self, timeout):
        polls = int(timeout * 50)
        while True:
            self._process.poll()
            if self._process.returncode is None:
                if polls > 0:
                    polls -= 1
                    time.sleep(0.02)
                else:
                    break
            else:
                return True
        return False

    def _interrupt(self, timeout=5.):
        try:
            os.kill(self._process.pid, INTERRUPT)
        except OSError as e:
            if e.errno == errno.ESRCH:  # No such process
                # Process already dead
                # Note: Windows won't complain if there is no such process
                return True
            else:
                raise
        return self._poll(timeout)

    def done(self):
        """Shut down the server, if running.

        If the server was never started, this method does nothing.

        If the server exited nicely (process exited with 0) or the script has
        been fully played, the server will be shut down gracefully.

        If the server exited with anything but 0, or a connection is open that
        cannot reach the end of the script, this functions terminates the
        process, dumps the output of the server and raises StubServerError


        Note about fully played scripts:
        If `<EXIT>` is invoked at any point in the script, this counts as
        finishing the script. Especially noteworthy, if `!AUTO: GOODBYE` is
        present, the client can reach the end of the script at any time by
        sending a `GOODBYE` message.
        """
        if not self._process:
            # test was probably skipped or failed before the stub server could
            # be started.
            return
        try:
            if self._poll(.1) or self._interrupt():
                pass
            elif self._interrupt():
                raise StubScriptNotFinishedError(
                    "Stub server didn't finish the script."
                )
            elif not self._interrupt():
                self._process.kill()
                self._process.wait()
                raise StubServerUncleanExitError("Stub server hanged.")
            if self._process.returncode not in (0, INTERRUPT_EXIT_CODE):
                if self._process.returncode == 3:
                    raise StubScriptNotFinishedError("Script never started.")
                raise StubServerUncleanExitError(
                    "Stub server exited unclean ({})".format(
                        self._process.returncode
                    )
                )
        except Exception:
            self._dump()
            raise
        finally:
            self._read_pipes()
            self._clean_up()

    def reset(self):
        """Make sure the sever is stopped and ready to start a new script.

        This method gives the server little time to gracefully shutdown, before
        sending a SIGKILL.

        If the server exited unexpectedly (e.g., script mismatch), dump the
        output.
        """
        if self._process:
            # give it some time to fully shut down if there was an error
            self._poll(0.1)
            # briefly try to get a shutdown that will dump script mismatches
            self._interrupt(.3)
            self._interrupt(0)
            self._kill()

    def _wait_for_silence(self, seconds, max_wait_seconds=5):
        max_wait = int(max_wait_seconds / (seconds / 2) + .5)
        self._read_pipes()
        buf_lens = len(self._stdout_lines), len(self._stderr_lines)
        last_change_before = 0
        while self._process and max_wait:
            max_wait -= 1
            time.sleep(seconds / 2)
            self._read_pipes()
            new_buf_lens = len(self._stdout_lines), len(self._stderr_lines)
            if buf_lens != new_buf_lens:
                last_change_before = 0
            else:
                last_change_before += 1
            if last_change_before >= 2:
                break
            buf_lens = new_buf_lens

    def get_negotiated_bolt_version(self):
        handshake_prefix = "<HANDSHAKE>"
        handshakes = self.get_responses("<HANDSHAKE>")
        if not handshakes:
            return 0,
        assert len(handshakes) == 1
        handshake = handshakes[0][len(handshake_prefix):]
        handshake = re.sub(r"\s", "", handshake)
        version = list(int(b, 16) for b in wrap(handshake, 2))
        while len(version) > 1 and not version[0]:
            version.pop(0)
        version.reverse()
        return tuple(version)

    def count_requests_re(self, pattern, silence_period=0.1):
        if isinstance(pattern, re.Pattern):
            return self.count_requests(pattern)
        return self.count_requests(re.compile(pattern),
                                   silence_period=silence_period)

    def count_requests(self, pattern, silence_period=0.1):
        return len(self.get_requests(pattern, silence_period))

    def get_requests(self, pattern, silence_period=0.1):
        self._wait_for_silence(silence_period)
        res = []
        for line in self._stdout_lines:
            # lines start with something like "10:08:33  [#EBE0>#2332]  "
            # plus some color escape sequences and ends on a newline
            line = re.sub(r"\x1b\[[\d;]+m", "", line[:-1])
            line = re.sub(r"^\d{2}:\d{2}:\d{2}\s+\[[0-9A-Fa-f#>]+\]\s+", "",
                          line)
            match = re.match(r"^(C: )|(\(\s*\d+\) C: )", line)
            if not match:
                continue
            line = line[match.end():]
            if isinstance(pattern, re.Pattern):
                if pattern.match(line):
                    res.append(line)
            else:
                if line.startswith(pattern):
                    res.append(line)
        return res

    def count_responses_re(self, pattern, silence_period=0.1):
        if isinstance(pattern, re.Pattern):
            return self.count_responses(pattern)
        return self.count_responses(re.compile(pattern),
                                    silence_period=silence_period)

    def get_responses(self, pattern, silence_period=0.1):
        self._wait_for_silence(silence_period)
        res = []
        for line in self._stdout_lines:
            # lines start with something like "10:08:33  [#EBE0>#2332]  "
            # plus some color escape sequences and ends on a newline
            line = re.sub(r"\x1b\[[\d;]+m", "", line[:-1])
            line = re.sub(r"^\d{2}:\d{2}:\d{2}\s+\[[0-9A-Fa-f#>]+\]\s+", "",
                          line)
            match = re.match(r"^(S: )|(\(\s*\d+\) S: )|(\(\s*\d+\)\s+)", line)
            if not match:
                continue
            line = line[match.end():]
            if isinstance(pattern, re.Pattern):
                if pattern.match(line):
                    res.append(line)
            else:
                if line.startswith(pattern):
                    res.append(line)
        return res

    def count_responses(self, pattern, silence_period=0.1):
        return len(self.get_responses(pattern, silence_period))

    @property
    def stdout(self):
        self._read_pipes()
        return "\n".join(self._stdout_lines)

    @property
    def stderr(self):
        self._read_pipes()
        return "\n".join(self._stderr_lines)

    @property
    def pipes(self):
        self._read_pipes()
        return "\n".join(self._stdout_lines), "\n".join(self._stderr_lines)


scripts_path = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "scripts"
)
