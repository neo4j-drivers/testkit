#!/usr/bin/env python
# coding: utf-8

# Copyright (c) 2002-2016 "Neo Technology,"
# Network Engine for Objects in Lund AB [http://neotechnology.com]
#
# This file is part of Neo4j.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


from sys import exit

from argparse import ArgumentParser
from asyncio import get_event_loop
from logging import getLogger, INFO

from boltstub import BoltStubService
from boltstub.scripting import BoltScript, ScriptMismatch
from boltstub.watcher import watch


log = getLogger(__name__)


def main():
    parser = ArgumentParser(description="""\
Run a Bolt stub server.

The stub server process listens for an incoming client connection and will 
attempt to play through a pre-scripted exchange with that client. Any deviation
from that script will result in a non-zero exit code. This utility is primarily
useful for Bolt client integration testing.
""")
    parser.add_argument("-l", "--listen-addr",
                        help="The base address on which to listen for incoming connections "
                             "in INTERFACE:PORT format, where INTERFACE may be omitted "
                             "for 'localhost'. Each script (which doesn't specify an "
                             "explicit port number) will use subsequent ports. If "
                             "completely omitted, this defaults to "
                             "':17687'. The BOLT_LISTEN_ADDR environment variable may "
                             "be used as an alternative to this option. Scripts may also "
                             "specify their own explicit port numbers.")
    parser.add_argument("-t", "--timeout", type=float,
                        help="The number of seconds for which the stub server will run "
                             "before automatically terminating. If unspecified, the "
                             "server will wait for 30 seconds.")
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="Show more detail about the client-server exchange.")
    parser.add_argument("script", nargs="+")
    parsed = parser.parse_args()

    if parsed.verbose:
        watch("boltstub", INFO)

    async def a():
        scripts = map(BoltScript.load, parsed.script)
        service = BoltStubService(*scripts, listen_addr=parsed.listen_addr, timeout=parsed.timeout)
        try:
            service.start()
            await service.wait_started()
        finally:
            try:
                await service.wait_stopped()
            except ScriptMismatch as error:
                extra = ""
                if error.script.filename:
                    extra += " in {!r}".format(error.script.filename)
                if error.line_no:
                    extra += " at line {}".format(error.line_no)
                print("Script mismatch{}:\n{}".format(extra, error))
                exit(1)
            except TimeoutError as error:
                print(error)
                exit(2)

    try:
        loop = get_event_loop()
        loop.run_until_complete(a())
    except KeyboardInterrupt:
        exit(130)
    except Exception as e:
        log.error(" ".join(map(str, e.args)))
        log.error("\r\n")
        exit(99)
    else:
        exit(0)


if __name__ == "__main__":
    main()
