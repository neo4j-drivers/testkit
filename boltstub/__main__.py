#!/usr/bin/env python

# Copyright (c) "Neo4j,"
# Neo4j Sweden AB [https://neo4j.com]
#
# This file is part of Neo4j.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import platform
import signal
import sys
import threading
import time
from argparse import ArgumentParser
from logging import (
    getLogger,
    INFO,
)

from . import BoltStubService
from .parsing import (
    parse_file,
    ScriptFailure,
)
from .watcher import watch

log = getLogger(__name__)


if platform.system() == "Windows":
    exit_code = None
    exit_code_lock = threading.Lock()

    def exit_(code):
        global exit_code
        global exit_code_lock
        with exit_code_lock:
            exit_code = code

    def check_exit():
        global exit_code
        global exit_code_lock
        with exit_code_lock:
            if exit_code is not None:
                sys.exit(exit_code)
else:
    exit_ = sys.exit


def main():
    sigint_count = 0
    service = None

    def _main():
        nonlocal service

        parser = ArgumentParser(description="""\
    Run a Bolt stub server.

    The stub server process listens for an incoming client connection and will
    attempt to play through a pre-scripted exchange with that client. Any
    deviation from that script will result in a non-zero exit code. This
    utility is primarily useful for Bolt client integration testing.
    """)
        parser.add_argument(
            "-l", "--listen-addr",
            help="The base address on which to listen for incoming "
                 "connections in INTERFACE:PORT format, where INTERFACE may "
                 "be omitted for 'localhost'. Each script (which doesn't "
                 "specify an explicit port number) will use subsequent ports. "
                 "If completely omitted, this defaults to "
                 "':17687'. The BOLT_LISTEN_ADDR environment variable may "
                 "be used as an alternative to this option. Scripts may also "
                 "specify their own explicit port numbers."
        )
        parser.add_argument(
            "-t", "--timeout", type=float,
            help="The number of seconds for which the stub server will run "
            "before automatically terminating. If unspecified, the "
            "server will wait for 30 seconds."
        )
        parser.add_argument(
            "-v", "--verbose", action="store_true",
            help="Show more detail about the client-server exchange."
        )
        parser.add_argument("script", nargs="+")
        parsed = parser.parse_args()

        if parsed.verbose:
            watch("boltstub", INFO)

        scripts = map(parse_file, parsed.script)
        service = BoltStubService(*scripts, listen_addr=parsed.listen_addr,
                                  timeout=parsed.timeout)

        try:
            service.start()
        except Exception as e:
            log.error(" ".join(map(str, e.args)))
            log.error("\r\n")
            return exit_(99)

        if service.exceptions:
            for error in service.exceptions:
                extra = ""
                if hasattr(error, "script") and error.script.filename:
                    extra += " in {!r}".format(error.script.filename)
                if isinstance(error, ScriptFailure):
                    print("Script mismatch{}:\n{}\n".format(extra, error))
                else:
                    print("Error{}:\n{}\n".format(extra, error))

            return exit_(1)

        if service.timed_out:
            print("Timed out")
            return exit_(2)

        if not service.ever_acted:
            print("Script never started")
            return exit_(3)

        return exit_(0)

    def signal_handler(sig, frame):
        nonlocal service
        nonlocal sigint_count
        if service is None:
            return exit_(100)  # process killed way too young :'(
        sigint_count += 1
        if sigint_count == 1:
            print("1st SIGINT received. Trying to finish all running scripts.")
            service.try_skip_to_end_async()
        elif sigint_count == 2:
            print("2nd SIGINT received. Closing all connections.")
            service.close_all_connections_async()
        elif sigint_count >= 3:
            print("3rd SIGINT received. Hard exit.")
            return exit_(130)

    if platform.system() == "Windows":
        signal.signal(signal.SIGBREAK, signal_handler)
        # On Windows, the signal won't wake up blocking sys calls.
        # So we keep the main thread in a busy loop, run the stub server in a
        # separate thread and use an exit flag to stop the process if needed.
        threading.Thread(target=_main, daemon=True).start()
        while True:
            time.sleep(.1)
            check_exit()
    else:
        # Look how easy it can be :D
        signal.signal(signal.SIGINT, signal_handler)
        _main()


if __name__ == "__main__":
    main()
