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

from .bolt_protocol import (
    BoltProtocol,
    get_bolt_protocol,
)
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
        handshake_manifest=None,
        handshake_data=None,
        handshake_response_data=None,
        handshake_delay=None,
        eval_context=None
    ):
        self.wire = wire
        self.bolt_protocol = get_bolt_protocol(bolt_version, bolt_features)
        self.stream = PackStream(wire, self.bolt_protocol.packstream_version)
        self.log = log_cb
        self._handshake_handler_step = None
        self._handshake_handler_step_data = None
        self.handshake_manifest = handshake_manifest
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
        if self._handshake_handler_step is not None:
            return self._handshake_handler_step()

        if self.handshake_data is not None:
            return self._version_handshake_fixed()

        accepted_version = self._version_handshake_init()
        accepted_major, accepted_minor = accepted_version
        if accepted_major < 0xff:
            handshake_handler = self._version_handshake_no_manifest
        else:
            handshake_handler_name = (
                f"_version_handshake_manifest_v{accepted_minor}"
            )
            handshake_handler = getattr(self, handshake_handler_name, None)
            if handshake_handler is None:
                raise NotImplementedError(
                    f"Handshake manifest {self.handshake_manifest} "
                    "is not implemented"
                )

        self._delay_handshake()

        return handshake_handler(accepted_version)

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

    def _version_handshake_init(self, manifest_version=None):
        request = self.wire.read(16)
        self._log("C: <HANDSHAKE> %s", hex_repr(request))
        requested_versions = BoltProtocol.decode_versions(request)
        accepted_offer = None
        for requested_version in requested_versions:
            accepted_offer = self._check_requested_version(requested_version)
            if accepted_offer is not None:
                break
        if accepted_offer is None:
            self._abort_handshake()
            manifest_version_str = (
                f"<= {self.bolt_protocol.max_handshake_manifest_version}"
                if manifest_version is None else str(manifest_version)
            )
            raise ScriptFailure(
                "Failed handshake, stub server talks protocol "
                f"{self.bolt_protocol.protocol_version} "
                f"(manifest version {manifest_version_str})."
                f"Driver sent handshake: {hex_repr(request)}"
            )

        return accepted_offer

    def _check_requested_version(self, requested_version):
        supported_version = self.bolt_protocol.protocol_version
        major, minor, range_ = requested_version
        if major == 0xff:
            if range_ != 0:
                raise NotImplementedError(
                    "Handshake range negotiation is not yet implemented"
                )
            if self.handshake_manifest is None:
                if self.bolt_protocol.max_handshake_manifest_version == 0:
                    return None
                if minor <= self.bolt_protocol.max_handshake_manifest_version:
                    return major, minor
            elif self.handshake_manifest == minor:
                return major, minor
            return None
        elif self.handshake_manifest not in {0, None}:
            return None

        if not self.bolt_protocol.handshake_range_support:
            range_ = 0
        if not self.bolt_protocol.handshake_minor_support:
            minor = 0

        if (
            supported_version[0] == major
            and minor >= supported_version[1] >= minor - range_
        ):
            return supported_version

        for supported_equivalent in self.bolt_protocol.equivalent_versions:
            if (
                supported_equivalent[0] == major
                and minor >= supported_equivalent[1] >= minor - range_
            ):
                return supported_equivalent

        return None

    def _version_handshake_no_manifest(self, accepted_version):
        response = bytes((0, 0, accepted_version[1], accepted_version[0]))
        self.wire.write(response)
        self.wire.send()
        self._log("S: <HANDSHAKE> %s", hex_repr(response))

    def _version_handshake_manifest_v1(self, accepted_version):
        assert accepted_version == (0xff, 1)

        # write server-side version offer
        supported_version = self.bolt_protocol.protocol_version
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
        self._handshake_handler_step = self._handshake_v1_step1
        self._handshake_v1_step1()

    def _handshake_v1_step1(self):
        client_pick = self.wire.read(4)
        self._handshake_handler_step_data = [client_pick]
        self._handshake_handler_step = self._handshake_v1_step2
        self._handshake_v1_step2()

    def _handshake_v1_step2(self):
        feature_pick = bytearray(self.wire.read(1))
        self._handshake_handler_step_data.append(feature_pick)
        self._handshake_handler_step = self._handshake_v1_step3
        self._handshake_v1_step3()

    def _handshake_v1_step3(self):
        client_pick, feature_pick = self._handshake_handler_step_data
        while feature_pick[-1] & 0x80:
            feature_pick.extend(self.wire.read(1))
        self._handshake_handler_step = None
        self._handshake_handler_step_data = None

        supported_version = self.bolt_protocol.protocol_version
        version_offer = 0, 0, supported_version[1], supported_version[0]
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
                self.log("(%4i) C: %s", line_no, self._buffered_msg)
            else:
                self.log("(%4i) C: %s", self._buffered_msg)
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
        struct = self.bolt_protocol.get_auto_response(msg)
        self.log("(AUTO) S: %s", struct)
        self.stream.write_message(struct)
        self.stream.drain()

    def try_auto_consume(self, whitelist: Iterable[str]):
        next_msg = self.peek()
        if next_msg.name in whitelist:
            self._buffered_msg = None  # consume the message for real
            self.log("C: %s", next_msg)
            self.auto_respond(next_msg)
            return True
        return False
