from typing import Iterable

from .bolt_protocol import get_bolt_protocol
from .errors import ServerExit
from .packstream import PackStream
from .parsing import ScriptFailure
from .util import hex_repr


class Channel:
    def __init__(self, wire, bolt_version, log_cb=None, handshake_data=None):
        self.wire = wire
        self.bolt_protocol = get_bolt_protocol(bolt_version)
        self.stream = PackStream(wire, self.bolt_protocol)
        self.log = log_cb
        self.handshake_data = handshake_data
        self._buffered_msg = None

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
        request = self.wire.read(16)
        self._log("C: <HANDSHAKE> %s", hex_repr(request))
        if self.handshake_data is not None:
            response = self.handshake_data
        else:
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
                    try:
                        self._log("S: <HANDSHAKE> %s",
                                  hex_repr(b"\x00\x00\x00\x00"))
                        self.wire.write(b"\x00\x00\x00\x00")
                        self.wire.send()
                    except OSError:
                        pass
                    raise ScriptFailure(
                        "Failed handshake, stub server talks protocol {}. "
                        "Driver sent handshake: {}".format(supported_version,
                                                           hex_repr(request))
                    )
        self.wire.write(response)
        self.wire.send()
        self._log("S: <HANDSHAKE> %s", hex_repr(response))

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
            if line_no:
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
