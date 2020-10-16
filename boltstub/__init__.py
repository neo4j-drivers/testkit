#!/usr/bin/env python
# coding: utf-8

# Copyright (c) 2002-2020 "Neo Technology,"
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


from logging import getLogger
from socketserver import TCPServer, BaseRequestHandler
from sys import stdout

from boltstub.addressing import Address
from boltstub.packstream import PackStream
from boltstub.scripting import ServerExit, ScriptMismatch, BoltScript, \
    ClientMessageLine
from boltstub.wiring import Wire


log = getLogger(__name__)


class BoltStubServer(TCPServer):

    allow_reuse_address = True

    timed_out = False

    def __init__(self, *args, **kwargs):
        super(BoltStubServer, self).__init__(*args, **kwargs)

    def handle_timeout(self):
        self.timed_out = True

    def server_bind(self):
        super(BoltStubServer, self).server_bind()
        # Must be here, testkit waits for something to be written on stdout to know when the server is listening.
        print("Listening")
        stdout.flush()


class BoltStubService:

    default_base_port = 17687

    default_timeout = 30

    thread = None

    auth = ("neo4j", "")

    @classmethod
    def load(cls, *script_filenames, **kwargs):
        return cls(*map(BoltScript.load, script_filenames), **kwargs)

    def __init__(self, script, listen_addr=None, exit_on_disconnect=True, timeout=None):
        if listen_addr:
            listen_addr = Address.parse(listen_addr)
        else:
            listen_addr = Address(("localhost", self.default_base_port))
        self.exit_on_disconnect = exit_on_disconnect
        self.host = listen_addr.host
        if script.port:
            self.address = Address((listen_addr.host, script.port))
        else:
            self.address = Address((listen_addr.host, listen_addr.port_number))
        self.script = script
        self.exceptions = []
        service = self

        class BoltStubRequestHandler(BaseRequestHandler):
            wire = None
            client_address = None
            server_address = None

            def setup(self):
                self.wire = Wire(self.request)
                self.client_address = self.wire.remote_address
                self.server_address = self.wire.local_address
                log.info("[#%04X]  S: <ACCEPT> %s -> %s", self.server_address.port_number,
                         self.client_address, self.server_address)

            def handle(self):
                try:
                    request = self.wire.read(20)
                    log.info("[#%04X]  C: <HANDSHAKE> %r", self.server_address.port_number, request)
                    response = script.on_handshake(request)
                    log.info("[#%04X]  S: <HANDSHAKE> %r", self.server_address.port_number, response)
                    self.wire.write(response)
                    self.wire.send()
                    actor = BoltActor(script, self.wire)
                    actor.play()
                except ServerExit:
                    pass
                except Exception as e:
                    service.exceptions.append(e)

            def finish(self):
                log.info("[#%04X]  S: <HANGUP>", self.wire.local_address.port_number)
                try:
                    self.wire.close()
                except OSError:
                    pass
                except AttributeError:
                    pass

        self.server = BoltStubServer(self.address, BoltStubRequestHandler)
        self.server.timeout = timeout or self.default_timeout

    def start(self):
        self.server.handle_request()

    @property
    def timed_out(self):
        return self.server.timed_out


class BoltActor:

    def __init__(self, script, wire):
        self.script = script
        self.wire = wire
        self.stream = PackStream(wire)

    @property
    def server_address(self):
        return self.wire.local_address

    def play(self):
        protocol_version = self.script.protocol_version
        try:
            for line in self.script:
                if not line.is_compatible(protocol_version):
                    raise ValueError("Script line %s is not compatible "
                                     "with protocol version %r" % (line, protocol_version))
                try:
                    line.action(self)
                except ScriptMismatch as error:
                    # Attach context information and re-raise
                    error.script = self.script
                    error.line_no = line.line_no
                    raise
        except (ConnectionError, OSError):
            # It's likely the client has gone away, so we can
            # safely drop out and silence the error. There's no
            # point in flagging a broken client from a test helper.
            return

    def log(self, text, *args):
        log.info("[#%04X]  " + text, self.server_address.port_number, *args)

    def log_error(self, text, *args):
        log.error("[#%04X]  " + text, self.server_address.port_number, *args)
