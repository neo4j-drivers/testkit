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


from copy import deepcopy
from logging import getLogger
from socketserver import TCPServer, ThreadingMixIn, BaseRequestHandler
from sys import stdout
from threading import (
    Lock,
    Thread,
)
import time
import traceback

from .addressing import Address
from .channel import Channel
from .errors import ServerExit
from .packstream import PackStream
from .parsing import (
    parse_file,
    Script,
    ScriptDeviation,
)
from .wiring import (
    ReadWakeup,
    Wire,
)

log = getLogger(__name__)


class BoltStubServer(TCPServer):

    allow_reuse_address = True

    timed_out = False

    def __init__(self, *args, **kwargs):
        super(BoltStubServer, self).__init__(*args, **kwargs)

    def handle_timeout(self):
        self.timed_out = True

    def server_activate(self):
        super(BoltStubServer, self).server_activate()
        # Must be here, testkit waits for something to be written on stdout to
        # know when the server is listening.
        print("Listening")
        stdout.flush()


class ThreadedBoltStubServer(ThreadingMixIn, BoltStubServer):
    pass


class BoltStubService:

    default_base_port = 17687

    default_timeout = 30

    auth = ("neo4j", "")

    @classmethod
    def load(cls, *script_filenames, **kwargs):
        return cls(*map(parse_file, script_filenames), **kwargs)

    def __init__(self, script: Script, listen_addr=None, timeout=None):
        if listen_addr:
            listen_addr = Address.parse(listen_addr)
        else:
            listen_addr = Address(("localhost", self.default_base_port))
        self.host = listen_addr.host
        self.address = Address((listen_addr.host, listen_addr.port_number))
        self.script = script
        self.exceptions = []
        self.actors = []
        self.ever_acted = False
        self.actors_lock = Lock()
        service = self

        class BoltStubRequestHandler(BaseRequestHandler):
            wire = None
            client_address = None
            server_address = None

            def setup(self):
                self.wire = Wire(self.request, read_wake_up=True)
                self.client_address = self.wire.remote_address
                self.server_address = self.wire.local_address
                log.info("[#%04X>#%04X]  S: <ACCEPT> %s -> %s",
                         self.client_address.port_number,
                         self.server_address.port_number,
                         self.client_address, self.server_address)

            def handle(self):
                with service.actors_lock:
                    actor = BoltActor(deepcopy(script), self.wire)
                    service.actors.append(actor)
                    service.ever_acted = True
                try:
                    actor.play()
                except ServerExit as e:
                    log.info("[#%04X>#%04X]  S: <EXIT> %s",
                             self.client_address.port_number,
                             self.server_address.port_number, e)
                except ScriptDeviation as e:
                    e.script = script
                    service.exceptions.append(e)
                except Exception as e:
                    traceback.print_exc()
                    service.exceptions.append(e)
                finally:
                    with service.actors_lock:
                        service.actors.remove(actor)

            def finish(self):
                log.info("[#%04X>#%04X]  S: <HANGUP>",
                         self.client_address.port_number,
                         self.server_address.port_number)
                try:
                    self.wire.close()
                except OSError:
                    pass
                except AttributeError:
                    pass

        if self.script.context.concurrent:
            server_cls = ThreadedBoltStubServer
        else:
            server_cls = BoltStubServer
        self.server = server_cls(self.address, BoltStubRequestHandler)
        self.server.timeout = timeout or self.default_timeout

    def start(self):
        if self.script.context.restarting or self.script.context.concurrent:
            self.server.serve_forever()
        else:
            self.server.handle_request()
            self.server.server_close()

    def _close_socket(self):
        self.server.socket.close()

    def _stop_server(self):
        if self.script.context.restarting or self.script.context.concurrent:
            self.server.shutdown()

    def stop(self):
        self._close_socket()
        self._stop_server()

    def try_skip_to_end(self):
        self._close_socket()
        with self.actors_lock:
            for actor in self.actors:
                actor.try_skip_to_end()
        self._stop_server()

    def try_skip_to_end_async(self):
        Thread(target=self.try_skip_to_end, daemon=True).start()

    def close_all_connections(self):
        self._close_socket()
        with self.actors_lock:
            for actor in self.actors:
                actor.exit()
        self._stop_server()

    def close_all_connections_async(self):
        Thread(target=self.close_all_connections, daemon=True).start()

    @property
    def timed_out(self):
        return self.server.timed_out


class BoltActor:

    def __init__(self, script: Script, wire):
        self.script = script
        self.channel = Channel(
            wire, script.context.bolt_version, log_cb=self.log,
            handshake_data=self.script.context.handshake
        )
        self._exit = False

    def play(self):
        for init_fn in (self.channel.preamble, self.channel.version_handshake):
            while True:
                if self._exit:
                    raise ServerExit("Actor exit on request")
                try:
                    init_fn()
                    break
                except ReadWakeup:
                    continue
        try:
            self.script.init(self.channel)
            while True:
                if self._exit:
                    raise ServerExit("Actor exit on request")
                if self.script.done():
                    break
                try:
                    self.script.consume(self.channel)
                except ReadWakeup:
                    # The `Script` class does some locking to protect its state
                    # which can be changed concurrently for example by
                    # `try_skip_to_end` being called from the interrupt handler
                    # in `__main__.py` which spawns a new thread. Without this
                    # `sleep`, the main thread that keeps calling
                    # `script.consume` only releases Script's internal lock only
                    # so briefly that `try_skip_to_end` hangs unnecessarily long
                    time.sleep(0.000001)
                    continue
        except (ConnectionError, OSError):
            # It's likely the client has gone away, so we can
            # safely drop out and silence the error. There's no
            # point in flagging a broken client from a test helper.
            return
        self.log("Script finished")

    def try_skip_to_end(self):
        self.script.try_skip_to_end()

    def exit(self):
        self._exit = True

    def log(self, text, *args):
        log.info("[#%04X>#%04X]  " + text,
                 self.channel.wire.remote_address.port_number,
                 self.channel.wire.local_address.port_number,
                 *args)

    def log_error(self, text, *args):
        log.error("[#%04X>#%04X]  " + text,
                  self.channel.wire.remote_address.port_number,
                  self.channel.wire.local_address.port_number,
                  *args)
