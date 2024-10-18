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


import traceback
from time import sleep
from typing import Iterable

from .bolt_protocol import get_bolt_protocol
from .errors import ServerExit
from .packstream import PackStream
from .parsing import ScriptFailure
from .util import (
    EvalContext,
    hex_repr,
)


class Channel:
    # This class is the glue between a stub script, the socket, and the bolt
    # protocol.

    def __init__(
        self,
        wire,
        bolt_version,
        bolt_features,
        log_cb=None,
        handshake_data=None,
        handshake_response_data=None,
        handshake_delay=None,
        eval_context=None
    ):
        self.wire = wire
        self.bolt_protocol = get_bolt_protocol(bolt_version, bolt_features)
        self.stream = PackStream(wire, self.bolt_protocol.packstream_version)
        self.log = log_cb
        self.handshake_data = handshake_data
        self.handshake_response_data = handshake_response_data
        self.handshake_delay = handshake_delay
        self._buffered_msg = None
        self.eval_context = eval_context or EvalContext()

    def _log(self, *args, **kwargs):
        if self.log:
            self.log(*args, **kwargs)

    def preamble(self):
        request = self.wire.read(4)
        self._log("C: <MAGIC> %s", hex_repr(request))
        if request != b"\x60\x60\xb0\x17":
            raise ServerExit(
                "Expected the magic header {}, received {}".format(
                    "6060B017", hex_repr(request)
                )
            )

    def version_handshake(self):
        if self.handshake_data is not None:
            return self._version_handshake_fixed()
        handshake_handler_name = (
            f"_version_handshake_v{self.bolt_protocol.handshake_version}"
        )
        handshake_handler = getattr(self, handshake_handler_name, None)
        if (
            handshake_handler is None
            or self.bolt_protocol.handshake_version is None
        ):
            raise NotImplementedError(
                f"Handshake version {self.bolt_protocol.handshake_version} "
                "is not implemented"
            )
        return handshake_handler()

    def _version_handshake_fixed(self):
        request = self.wire.read(16)
        self._log("C: <HANDSHAKE> %s", hex_repr(request))
        self._delay_handshake()
        response = self.handshake_data
        self.wire.write(response)
        self.wire.send()
        self._log("S: <HANDSHAKE> %s", hex_repr(response))
        if self.handshake_response_data is not None:
            client_response = self.wire.read(len(self.handshake_response_data))
            if client_response != self.handshake_response_data:
                raise ServerExit(
                    "Expected the client handshake response "
                    f"{hex_repr(self.handshake_response_data)}, received "
                    f"{hex_repr(client_response)}"
                )

    def _version_handshake_v1(self):
        request = self.wire.read(16)
        self._log("C: <HANDSHAKE> %s", hex_repr(request))
        # Check that the server protocol version is among the ones
        # supported by the driver.
        supported_version = self.bolt_protocol.protocol_version
        requested_versions = set(
            self.bolt_protocol.decode_versions(request)
        )
        if supported_version in requested_versions:
            response = bytes(
                (0, 0, supported_version[1], supported_version[0])
            )
        else:
            fallback_versions = (requested_versions
                                 & self.bolt_protocol.equivalent_versions)
            if fallback_versions:
                version = sorted(fallback_versions, reverse=True)[0]
                response = bytes((0, 0, version[1], version[0]))
            else:
                self._abort_handshake()
                raise ScriptFailure(
                    "Failed handshake, stub server talks protocol "
                    f"{supported_version}. Driver sent handshake: "
                    f"{hex_repr(request)}"
                )
        self._delay_handshake()
        self.wire.write(response)
        self.wire.send()
        self._log("S: <HANDSHAKE> %s", hex_repr(response))

    def _version_handshake_v2(self):
        request = self.wire.read(16)
        self._log("C: <HANDSHAKE> %s", hex_repr(request))
        supported_version = self.bolt_protocol.protocol_version
        requested_versions = set(
            self.bolt_protocol.decode_versions(request)
        )
        if (0xff, 1) not in requested_versions:
            self._abort_handshake()
            raise ScriptFailure(
                "Failed handshake, expected handshake version 2 offer "
                f"(00 00 01 FF) received {hex_repr(request)}"
            )
        self._delay_handshake()
        # write server-side version offer
        version_offer = 0, 0, supported_version[1], supported_version[0]
        full_offer = (
            # negotiate handshake version 2
            0, 0, 1, 0xff,
            # announce number of server-side offered protocol versions
            1,
            # server-side offered protocol version(s)
            *version_offer,
            # feature flag(s)
            *self.bolt_protocol.features,
        )
        self.wire.write(full_offer)
        self._log(
            "S: <HANDSHAKE> 00 00 01 FF [1] %s %s",
            hex_repr(version_offer),
            hex_repr(self.bolt_protocol.features)
        )
        self.wire.send()
        client_pick = self.wire.read(4)
        feature_pick = bytearray(self.wire.read(1))
        while feature_pick[-1] & 0x80:
            feature_pick.extend(self.wire.read(1))
        self._log(
            "C: <HANDSHAKE> %s %s",
            hex_repr(client_pick),
            hex_repr(feature_pick),
        )
        if client_pick != bytes(version_offer):
            raise ScriptFailure(
                "Failed handshake, client picked different version "
                f"{hex_repr(client_pick)} than offered "
                f"{hex_repr(version_offer)}"
            )
        if feature_pick != self.bolt_protocol.features:
            raise ScriptFailure(
                "Failed handshake, client picked different features "
                f"({hex_repr(feature_pick)}) than offered "
                f"({hex_repr(self.bolt_protocol.features)})"
            )

    def _abort_handshake(self):
        self._log("S: <HANDSHAKE> %s", hex_repr(b"\x00\x00\x00\x00"))
        try:
            self.wire.write(b"\x00\x00\x00\x00")
            self.wire.send()
        except OSError:
            pass

    def _delay_handshake(self):
        if self.handshake_delay:
            self._log("S: <HANDSHAKE DELAY> %s", self.handshake_delay)
            sleep(self.handshake_delay)

    def match_client_line(self, client_line, msg):
        return client_line.match_message(msg.name, msg.fields)

    def send_raw(self, b):
        self.log("%s", hex_repr(b))
        self.wire.write(b)
        self.wire.send()

    def send_struct(self, struct):
        self.log("S: %s", struct)
        self.stream.write_message(struct)
        self.stream.drain()

    def send_server_line(self, server_line):
        self.log("%s", server_line)
        server_line = self.bolt_protocol.translate_server_line(server_line)
        self.stream.write_message(server_line)
        self.stream.drain()

    def _consume(self):
        return self.bolt_protocol.translate_structure(
            self.stream.read_message()
        )

    def consume(self, line_no=None):
        if self._buffered_msg is not None:
            if line_no is not None:
                self.log("(%3i) C: %s", line_no, self._buffered_msg)
            else:
                self.log("(%3i) C: %s", self._buffered_msg)
            msg = self._buffered_msg
            self._buffered_msg = None
            return msg
        return self._consume()

    def peek(self):
        if self._buffered_msg is None:
            self._buffered_msg = self._consume()
        return self._buffered_msg

    def assert_no_input(self):
        no_input = self.wire.check_no_input()
        if not no_input:
            try:
                msg = self.peek()
            except Exception:
                msg = (
                    "some data (encountered error while trying to peek):\n"
                    f"{traceback.format_exc()}"
                )
            self.wire.close()
            raise ScriptFailure(
                "Expected the driver to not send anything, but received: "
                f"{msg}",
            )

    def auto_respond(self, msg):
        self.log("AUTO response:")
        self.send_struct(self.bolt_protocol.get_auto_response(msg))

    def try_auto_consume(self, whitelist: Iterable[str]):
        next_msg = self.peek()
        if next_msg.name in whitelist:
            self._buffered_msg = None  # consume the message for real
            self.log("C: %s", next_msg)
            self.auto_respond(next_msg)
            return True
        return False
