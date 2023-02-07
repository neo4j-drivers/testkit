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


"""
Low-level module for network communication.

This module provides a convenience socket wrapper class (:class:`.Wire`)
as well as classes for modelling IP addresses, based on tuples.
"""


import base64
from functools import cached_property
import hashlib
from socket import (
    AF_INET,
    AF_INET6,
    getservbyname,
    timeout,
)
import struct

BOLT_PORT_NUMBER = 7687
HTTP_HEADER_MIN_SIZE = 26  # BYTES
MAGIC_WS_STRING = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
PONG = b"\x0A\x00"


class ReadWakeup(timeout):
    pass


class Address(tuple):
    """Address of a machine on a network."""

    @classmethod
    def parse(cls, s, default_host=None, default_port=None):
        if not isinstance(s, str):
            raise TypeError("Address.parse requires a string argument")
        if s.startswith("["):
            # IPv6
            host, _, port = s[1:].rpartition("]")
            port = port.lstrip(":")
            try:
                port = int(port)
            except (TypeError, ValueError):
                pass
            return cls((host or default_host or "localhost",
                        port or default_port or 0, 0, 0))
        else:
            # IPv4
            host, _, port = s.partition(":")
            try:
                port = int(port)
            except (TypeError, ValueError):
                pass
            return cls((host or default_host or "localhost",
                        port or default_port or 0))

    def __new__(cls, iterable):
        if isinstance(iterable, cls):
            return iterable
        n_parts = len(iterable)
        inst = tuple.__new__(cls, iterable)
        if n_parts == 2:
            inst.__class__ = IPv4Address
        elif n_parts == 4:
            inst.__class__ = IPv6Address
        else:
            raise ValueError("Addresses must consist of either "
                             "two parts (IPv4) or four parts (IPv6)")
        return inst

    #: Address family (AF_INET or AF_INET6)
    family = None

    def __repr__(self):
        return "{}({!r})".format(self.__class__.__name__, tuple(self))

    @property
    def host(self):
        return self[0]

    @property
    def port(self):
        return self[1]

    @property
    def port_number(self):
        if self.port == "bolt":
            # Special case, just because. The regular /etc/services
            # file doesn't contain this, but it can be found in
            # /usr/share/nmap/nmap-services if nmap is installed.
            return BOLT_PORT_NUMBER
        try:
            return getservbyname(self.port)
        except (OSError, TypeError):
            # OSError: service/proto not found
            # TypeError: getservbyname() argument 1 must be str, not X
            try:
                return int(self.port)
            except (TypeError, ValueError) as e:
                raise type(e)("Unknown port value %r" % self.port)


class IPv4Address(Address):
    """Address subclass, specifically for IPv4 addresses."""

    family = AF_INET

    def __str__(self):
        return "{}:{}".format(*self)


class IPv6Address(Address):
    """Address subclass, specifically for IPv6 addresses."""

    family = AF_INET6

    def __str__(self):
        return "[{}]:{}".format(*self)


class RegularSocket:
    """A socket with an already receive cached value."""

    def __init__(self, socket_, cache) -> None:
        self._socket = socket_
        self._cache = cache

    def __getattr__(self, item):
        return getattr(self._socket, item)

    def recv(self, bufsize) -> bytes:
        if self._cache:
            buff = self._cache
            self._cache = None
            return buff
        return self._socket.recv(bufsize)


class WebSocket:
    """Implementation of Websockets [rfc6455].

    This implementation doesn't support extensions
    """

    def __init__(self, socket_) -> None:
        self._socket = socket_

    def __getattr__(self, item):
        return getattr(self._socket, item)

    def recv(self, bufsize) -> bytes:
        """Receive data from the socket.

        The `bufsize` parameter is ignored. This method returns the entire
        frame payload and handles control frames doing the needed actions.
        """
        frame = self._socket.recv(2)
        if len(frame) == 0:
            return None

        fin = frame[0] >> 7
        # rsv1 = frame[0] & 0b0100_0000 == 0b0100_0000
        # rsv2 = frame[0] & 0b0010_0000 == 0b0010_0000
        # rsv3 = frame[0] & 0b0001_0000 == 0b0001_0000
        opcode = frame[0] & 0b0000_1111

        masked = frame[1] >> 7
        payload_len = frame[1] & 0b0111_1111

        if payload_len == 126:
            payload_len, = struct.unpack(">H", self._socket.recv(2))
        elif payload_len == 127:
            payload_len, = struct.unpack(">Q", self._socket.recv(8))

        if masked == 1:
            mask = self._socket.recv(4)

        masked_payload = self._socket.recv(payload_len)

        payload = masked_payload \
            if masked == 0 \
            else bytearray([masked_payload[i] ^ mask[i % 4]
                            for i in range(payload_len)])
        if opcode & 0b0000_1000 == 0b0000_1000:
            if opcode == 0x09:  # PING
                self._socket.sendall(PONG)
            return self.recv(bufsize)
        elif fin == 0:
            return payload + self.recv(bufsize)
        return payload

    def send(self, payload) -> int:
        """Send the payload over the socket inside a Websocket frame."""
        frame = [0b1000_0010]
        payload_len = len(payload)
        if payload_len < 126:
            frame += [payload_len]
        elif payload_len < 0x10000:
            frame += [126]
            frame += bytearray(struct.pack(">H", payload_len))
        else:
            frame += [127]
            frame += bytearray(struct.pack(">Q", payload_len))

        frame_to_send = bytearray(frame) + bytearray(payload)

        self._socket.sendall(frame_to_send)

        return len(payload)

    sendall = send


class Wire(object):
    """Buffered socket wrapper for reading and writing bytes."""

    _closed = False

    _broken = False

    def __init__(self, s, read_wake_up=False):
        # ensure wrapped socket is in blocking mode but wakes up occasionally
        # if wake_up == True
        s.settimeout(.1 if read_wake_up else None)
        self._socket = s
        self._input = bytearray()
        self._output = bytearray()

    def secure(self, verify=True, hostname=None):
        """Apply a layer of security onto this connection."""
        from ssl import (
            CERT_NONE,
            CERT_REQUIRED,
            PROTOCOL_TLS,
            SSLContext,
        )
        context = SSLContext(PROTOCOL_TLS)
        if verify:
            context.verify_mode = CERT_REQUIRED
            context.check_hostname = bool(hostname)
        else:
            context.verify_mode = CERT_NONE
        context.load_default_certs()
        try:
            self._socket = context.wrap_socket(self._socket,
                                               server_hostname=hostname)
        except OSError:
            # TODO: add connection failure/diagnostic callback
            raise WireError(
                "Unable to establish secure connection with remote peer"
            )

    def read(self, n):
        """Read bytes from the network."""
        while len(self._input) < n:
            required = n - len(self._input)
            requested = max(required, 8192)
            try:
                received = self._socket.recv(requested)
            except timeout:
                raise ReadWakeup
            except OSError:
                self._broken = True
                raise BrokenWireError("Broken")
            else:
                if received:
                    self._input.extend(received)
                else:
                    self._broken = True
                    raise BrokenWireError("Network read incomplete "
                                          "(received %d of %d bytes)" %
                                          (len(self._input), n))
        data = self._input[:n]
        self._input[:n] = []
        return data

    def write(self, b):
        """Write bytes to the output buffer."""
        self._output.extend(b)

    def send(self):
        """Send the contents of the output buffer to the network."""
        if self._closed:
            raise WireError("Closed")
        sent = 0
        while self._output:
            try:
                n = self._socket.send(self._output)
            except timeout:
                continue
            except OSError:
                self._broken = True
                raise BrokenWireError("Broken")
            else:
                self._output[:n] = []
                sent += n
        return sent

    def close(self):
        """Close the connection."""
        try:
            # TODO: shutdown
            self._socket.close()
        except OSError:
            self._broken = True
            raise BrokenWireError("Broken")
        else:
            self._closed = True

    @property
    def closed(self):
        """Flag indicating whether this connection has been closed locally."""
        return self._closed

    @property
    def broken(self):
        """Flag indicating whether this connection has been closed remotely."""
        return self._broken

    @cached_property
    def local_address(self):
        """Get the local address to which this connection is bound.

        :rtype: Address
        """
        return Address(self._socket.getsockname())

    @cached_property
    def remote_address(self):
        """Get the remote address to which this connection is bound.

        :rtype: Address
        """
        return Address(self._socket.getpeername())


class WireError(OSError):
    """Raised when a connection error occurs."""


class BrokenWireError(WireError):
    """Raised when a connection is broken by the network or remote peer."""


def negotiate_socket(socket_):
    def try_to_negotiate_websocket(socket__, buffer_):
        encoding = "utf-8"
        encoded = buffer_.strip().decode(encoding)
        headers = encoded.strip().split("\r\n")
        if "Upgrade: websocket" not in headers:
            return False
        for h in headers:
            if "Sec-WebSocket-Key" in h:
                key = h.split(" ")[1]
        key = key + MAGIC_WS_STRING
        encoded_key = key.encode(encoding)
        encrypted_key = hashlib.sha1(encoded_key).digest()
        base64_key = base64.standard_b64encode(encrypted_key).decode(encoding)
        response = ("HTTP/1.1 101 Switching Protocols\r\n"
                    + "Upgrade: websocket\r\n"
                    + "Connection: Upgrade\r\n"
                    + "Sec-WebSocket-Accept: %s\r\n\r\n") % base64_key
        encoded_response = response.encode(encoding)
        socket__.sendall(encoded_response)
        return True

    buffer = socket_.recv(1024)
    if len(buffer) >= HTTP_HEADER_MIN_SIZE:
        negotiated = try_to_negotiate_websocket(socket_, buffer)
        if negotiated:
            return WebSocket(socket_)

    return RegularSocket(socket_, buffer)


def create_wire(s, read_wake_up, wrap_socket=negotiate_socket):
    actual_socket = wrap_socket(s)
    return Wire(actual_socket, read_wake_up)
