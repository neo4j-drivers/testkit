#!/usr/bin/env python

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
from socket import socket
from socketserver import (
    BaseRequestHandler,
    TCPServer,
    ThreadingMixIn,
)
import struct
from sys import stdout
from threading import (
    Lock,
    Thread,
)
import time
import traceback
import base64
import hashlib
from .addressing import Address
from .channel import Channel
from .errors import ServerExit
from .packstream import PackStream
from .parsing import (
    parse_file,
    Script,
    ScriptFailure,
)
from .wiring import (
    ReadWakeup,
    Wire,
)

log = getLogger(__name__)

MAGIC_WS_STRING = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"


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


class WebSocket (socket):

    def __init__(self, _socket):
        self._socket = _socket

    def getsockname(self):
        return self._socket.getsockname()

    def getpeername(self):
        return self._socket.getpeername()

    def close(self) -> None:
        return self._socket.close()

    def settimeout(self, __value: float) -> None:
        pass

    def recv(self, __bufsize) -> bytes:
        frame = self._socket.recv(2)
        if len(frame) == 0:
            return None

        fin = frame[0] >> 7
        rsv1 = frame[0] & 0b0100_0000 == 0b0100_0000
        rsv2 = frame[0] & 0b0010_0000 == 0b0010_0000
        rsv3 = frame[0] & 0b0001_0000 == 0b0001_0000
        opcode = frame[0] & 0b0000_1111

        masked = frame[1] >> 7
        payload_len = frame[1] & 0b0111_1111

        if payload_len == 126:
            payload_len = struct.unpack(">H", self._socket.recv(2))
            # print(payload_len)
            # raise Exception('16bit')
        elif payload_len == 127:
            payload_len = struct.unpack(">Q", self._socket.recv(8))
            # print(payload_len)
            # raise Exception('64bit')

        if masked == 1:
            mask = self._socket.recv(4)

        masked_payload = self._socket.recv(payload_len)

        payload = masked_payload \
            if mask == 0 \
            else bytearray([masked_payload[i] ^ mask[i % 4]
                            for i in range(payload_len)])

        return payload

    def send(self, payload) -> int:
        frame = [0b1000_0010]
        payload_len = len(payload)
        should_raise = False
        if (payload_len < 126):
            frame += [payload_len]
        elif payload_len <= 0x10000:
            frame += [126]
            frame += bytearray(struct.pack(">H", payload_len))
        else:
            frame += [127]
            frame += bytearray(struct.pack(">Q", payload_len))

        frame_to_send = bytearray(frame) + bytearray(payload)

        self._socket.sendall(frame_to_send)

        return len(payload)


class WebSocketRequesetHandler(BaseRequestHandler):
    def handle(self) -> None:
        encoding = "utf-8"
        data = self.request.recv(1024).strip().decode(encoding)
        headers = data.strip().split("\r\n")
        if 'Upgrade: websocket' in headers:
            for h in headers:
                if "Sec-WebSocket-Key" in h:
                    key = h.split(" ")[1]
            key = key + MAGIC_WS_STRING

            resp_key = base64.standard_b64encode(
                hashlib.sha1(key.encode(encoding)).digest()).decode(encoding)

            self.request.sendall(("HTTP/1.1 101 Switching Protocols\r\n"
                                  + "Upgrade: websocket\r\n"
                                  + "Connection: Upgrade\r\n"
                                  + "Sec-WebSocket-Accept: %s\r\n\r\n" % (resp_key)).encode(encoding))
            self.request = WebSocket(self.request)


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

        class BoltStubRequestHandler(WebSocketRequesetHandler):
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

            def handle(self) -> None:
                super().handle()
                print('I am here')
                self.wire = Wire(self.request, read_wake_up=True)

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
                except ScriptFailure as e:
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
                    # `script.consume` only releases Script's internal lock  so
                    # briefly that `try_skip_to_end` hangs unnecessarily long
                    time.sleep(0.000001)
                    continue
        except OSError as e:
            # It's likely the client has gone away, so we can
            # safely drop out and silence the error. There's no
            # point in flagging a broken client from a test helper.
            self.log("S: <BROKEN> %r", e)
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
