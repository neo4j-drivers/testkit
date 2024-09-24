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


import socket
import threading
import time
import traceback
from contextlib import contextmanager
from struct import unpack as struct_unpack

import pytest

from .. import BoltStubService
from ..parsing import (
    parse,
    ScriptFailure,
)
from ..util import hex_repr
from ._common import (
    ALL_BOLT_VERSIONS,
    ALL_REQUESTS_PER_VERSION,
    ALL_RESPONSES_PER_VERSION,
    cycle_zip,
)


def server_version_to_version_response(server_version):
    return bytes(
        0 if i >= len(server_version) else server_version[i]
        for i in range(3, -1, -1)
    )


def server_version_to_version_request(server_version):
    return server_version_to_version_response(server_version) + b"\x00" * 12


class BrokenSocket(RuntimeError):
    pass


class Connection:
    def __init__(self, host, port):
        self._socket = socket.create_connection((host, port), timeout=0.2)
        self._socket.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, True)

    def read(self, byte_count):
        buffer = bytearray()
        while len(buffer) < byte_count:
            try:
                chunk = self._socket.recv(byte_count - len(buffer))
            except ConnectionResetError as e:
                raise BrokenSocket() from e
            if chunk == b"":
                raise BrokenSocket()
            buffer += chunk
        # print("<", buffer)
        return buffer

    def write(self, bytes_):
        # print(">", bytes_)
        self._socket.sendall(bytes_)

    def close(self):
        self._socket.close()

    def read_message(self):
        res = bytearray()
        chunk_header = self.read(2)
        chunk_size, = struct_unpack(">H", chunk_header)
        while chunk_size:
            res.extend(self.read(chunk_size))
            chunk_header = self.read(2)
            chunk_size, = struct_unpack(">H", chunk_header)
        return res

    def get_timeout(self):
        return self._socket.gettimeout()

    def set_timeout(self, timeout):
        return self._socket.settimeout(timeout)

    @contextmanager
    def timeout(self, timeout):
        old_timeout = self.get_timeout()
        try:
            self.set_timeout(timeout)
            yield
        finally:
            self.set_timeout(old_timeout)


class ThreadedServer(threading.Thread):
    def __init__(self, script, address):
        super().__init__(daemon=True)
        if isinstance(script, str):
            script = parse(script)
        self.service = BoltStubService(script, address, timeout=1)
        self.exc = None
        self._stopped = False

    def run(self):
        try:
            self.service.start()
        except Exception as e:
            traceback.print_exc()
            self.exc = e

    def stop(self):
        if self._stopped:
            return
        self._stopped = True
        self.service.stop()

    def join(self, timeout=None):
        super().join(timeout=timeout)


@pytest.fixture()
def server_factory():
    server = None

    def factory(script, address="localhost:7687"):
        nonlocal server
        if server is not None:
            raise RuntimeError("server already running")
        server = ThreadedServer(script, address)
        server.start()
        return server

    try:
        yield factory
    finally:
        if server is not None:
            server.stop()
            server.join()
            for exc in server.service.exceptions:
                traceback.format_exception(type(exc), exc, None)


@pytest.fixture()
def connection_factory():
    connections = []

    def factory(*args, **kwargs):
        con_ = Connection(*args, **kwargs)
        connections.append(con_)
        return con_

    yield factory
    for con in connections:
        con.close()


@pytest.mark.parametrize("server_version", ALL_BOLT_VERSIONS)
@pytest.mark.parametrize(("magic_bytes", "fail"), [
    (b"\x60\x60\xb0\x16", True),
    (b"\x60\x60\xb0\x18", True),
    (b"\x61\x60\xb0\x17", True),
    (b"\x60\x61\xb0\x17", True),
    (b"\x60\x60\xb0\x17", False),
])
def test_magic_bytes(server_version, magic_bytes, server_factory, fail,
                     connection_factory):
    script = parse("""
    !: BOLT {}

    C: RUN
    """.format(".".join(map(str, server_version))))
    server = server_factory(script)
    con = connection_factory("localhost", 7687)
    con.write(magic_bytes)
    if fail:
        with pytest.raises(BrokenSocket):
            con.read(1)
    else:
        with pytest.raises(socket.timeout):
            con.read(1)
    assert not server.service.exceptions


@pytest.mark.parametrize(("client_version", "server_version",
                          "negotiated_version"), [
    [b"\x00\x00\x00\x01", (1,), (1,)],
    [b"\x00\x00\x00\x01", (1, 0), (1, 0)],
    [b"\x00\x00\x00\x01\x00\x00\x00\x02", (1, 0), (1, 0)],
    [b"\x00\x00\x00\x01\x00\x00\x00\x02", (2, 0), (2, 0)],
    [b"\x00\x00\x00\x01\x00\x00\x00\x03", (2,), None],
    [b"\x00\x00\x00\x03", (3,), (3,)],
    [b"\x00\x00\x00\x04", (4,), (4,)],
    [b"\x00\x00\x01\x04", (4, 1), (4, 1)],
    [b"\x00\x00\x02\x04", (4, 2), (4, 2)],
    [b"\x00\x00\x03\x04\x00\x00\x02\x04\x00\x00\x01\x04", (4, 2), (4, 2)],
    [b"\x00\x00\x03\x04", (4, 3), (4, 3)],
    [b"\x00\x02\x03\x04", (4, 3), (4, 3)],
    [b"\x00\x03\x03\x04\x00\x00\x02\x04\x00\x00\x01\x04\x00\x00\x00\x03",
     (4, 0), None],
    [b"\x00\x01\x03\x04\x00\x00\x01\x04\x00\x00\x00\x04\x00\x00\x00\x03",
     (4, 0), (4, 0)],
    # ignore minor versions until 4.0
    [b"\x00\x00\x10\x01", (1,), (1,)],
    [b"\x00\x00\x10\x02", (2,), (2,)],
    [b"\x00\x00\x10\x03", (3,), (3,)],
    [b"\x00\x00\x10\x04", (4, 0), None],
    [b"\x00\x00\x10\x04", (4, 1), None],
    [b"\x00\x00\x10\x04", (4, 2), None],
    [b"\x00\x00\x10\x04", (4, 3), None],
    # ignore version ranges until 4.3
    [b"\x00\x09\x0A\x01", (1,), (1,)],
    [b"\x00\x0A\x0A\x01", (1,), (1,)],
    [b"\x00\x09\x0A\x02", (2,), (2,)],
    [b"\x00\x0A\x0A\x02", (2,), (2,)],
    [b"\x00\x09\x0A\x03", (3,), (3,)],
    [b"\x00\x0A\x0A\x03", (3,), (3,)],
    [b"\x00\x0A\x0A\x04", (4, 0), None],
    [b"\x00\x09\x0A\x04", (4, 0), None],
    [b"\x00\x0A\x0A\x04", (4, 1), None],
    [b"\x00\x09\x0A\x04", (4, 1), None],
    [b"\x00\x08\x0A\x04", (4, 1), None],
    [b"\x00\x09\x0A\x04", (4, 2), None],
    [b"\x00\x08\x0A\x04", (4, 2), None],
    [b"\x00\x07\x0A\x04", (4, 2), None],
    [b"\x00\x08\x0A\x04", (4, 3), (4, 3)],
    [b"\x00\x07\x0A\x04", (4, 3), (4, 3)],
    [b"\x00\x06\x0A\x04", (4, 3), None],
    # special backwards compatibility
    # (4.2 server allows to fall back to equivalent 4.1 protocol)
    [b"\x00\x00\x01\x04", (4, 2), (4, 1)],
    [b"\x00\x00\x02\x04", (4, 1), None],
    [b"\x00\x00\x02\x04", (4, 3), None],
])  # noqa: PAR102
def test_handshake_auto(client_version, server_version, negotiated_version,
                        server_factory, connection_factory):
    client_version = client_version + b"\x00" * (16 - len(client_version))

    script = parse("""
    !: BOLT {}

    C: RUN
    """.format(".".join(map(str, server_version))))

    server = server_factory(script)
    con = connection_factory("localhost", 7687)
    con.write(b"\x60\x60\xb0\x17")
    con.write(client_version)
    if negotiated_version is not None:
        assert (con.read(4)
                == server_version_to_version_response(negotiated_version))
    else:
        assert con.read(4) == b"\x00" * 4
        with pytest.raises(BrokenSocket):
            print(con.read(1))
    if negotiated_version is None:
        assert len(server.service.exceptions) == 1
        assert isinstance(server.service.exceptions[0], ScriptFailure)
    else:
        assert not server.service.exceptions


@pytest.mark.parametrize("custom_handshake", [b"\x00\x00\xFF\x00", b"foobar"])
@pytest.mark.parametrize("client_version", [b"\x00\x00\x00\x01", b"crap"])
@pytest.mark.parametrize("server_version", ALL_BOLT_VERSIONS)
def test_custom_handshake_auto(custom_handshake, client_version,
                               server_version, server_factory,
                               connection_factory):
    client_version = client_version + b"\x00" * (16 - len(client_version))

    script = parse("""
    !: BOLT {}
    !: HANDSHAKE {}

    C: RUN
    """.format(".".join(map(str, server_version)), hex_repr(custom_handshake)))

    server = server_factory(script)
    con = connection_factory("localhost", 7687)
    con.write(b"\x60\x60\xb0\x17")
    con.write(client_version)
    assert con.read(len(custom_handshake)) == custom_handshake
    with pytest.raises(socket.timeout):
        con.read(1)
    assert not server.service.exceptions


@pytest.mark.parametrize("delay", [0.5, 1.0])
@pytest.mark.parametrize("bound", ["upper", "lower"])
def test_handshake_delay(delay, bound, server_factory, connection_factory):
    script = parse("""
    !: BOLT 5.2
    !: HANDSHAKE_DELAY {}

    C: RUN
    """.format(delay))

    server = server_factory(script)
    con = connection_factory("localhost", 7687)
    con.write(b"\x60\x60\xb0\x17")
    con.write(b"\x00\x00\x02\x05" + b"\x00" * 12)
    if bound == "upper":
        with con.timeout(delay + 0.2):
            assert con.read(4) == b"\x00\x00\x02\x05"
    elif bound == "lower":
        with con.timeout(delay - 0.2):
            with pytest.raises(socket.timeout):
                con.read(4)
    assert not server.service.exceptions


@pytest.mark.parametrize(("server_version", "request_tag", "request_name"),
                         ALL_REQUESTS_PER_VERSION)
def test_auto_replies(server_version, request_tag, request_name,
                      server_factory, connection_factory):
    another_client_msg = next(n for v, _, n in ALL_REQUESTS_PER_VERSION
                              if v == server_version and n != request_name)
    script = parse("""
    !: BOLT {}
    !: AUTO {}

    C: {}
    """.format(".".join(map(str, server_version)), request_name,
               another_client_msg))

    server = server_factory(script)
    con = connection_factory("localhost", 7687)
    con.write(b"\x60\x60\xb0\x17")
    con.write(server_version_to_version_request(server_version))
    con.read(4)
    con.write(b"\x00\x02\xb0" + request_tag + b"\x00\x00")
    res = con.read_message()
    assert res[:2] == b"\xb1\x70"  # SUCCESS
    with pytest.raises(socket.timeout):
        con.read(1)
    assert not server.service.exceptions


@pytest.mark.parametrize(("server_version", "request_tag", "request_name",
                          "field_rep", "field_bin"), (
    *((version, tag, name, rep, fields)
      for (version, tag, name), (rep, fields) in
      cycle_zip(ALL_REQUESTS_PER_VERSION, (
          ("null", b"\xC0"),
          ("false", b"\xC2"),
          ('{"?": true}', b"\xC3"),
          ('{"Z": "42"}', b"\x2A"),
          ('{"Z": "42"}', b"\xC8\x2A"),
          ("42", b"\xC9\x00\x2A"),
          ('{"Z": "42"}', b"\xCA\x00\x00\x00\x2A"),
          ('{"Z": "42"}', b"\xCB\x00\x00\x00\x00\x00\x00\x00\x2A"),
          ('{"R": "1.23"}', b"\xC1\x3F\xF3\xAE\x14\x7A\xE1\x47\xAE"),
          ('{"#": "2A"}', b"\xCC\x01\x2A"),
          ('{"#": "2A"}', b"\xCD\x00\x01\x2A"),
          ('{"#": "2A"}', b"\xCE\x00\x00\x00\x01\x2A"),
          ('{"U": "abc"}', b"\x83abc"),
          ('"foobar"', b"\x86foobar"),
          ('{"U": "abc"}', b"\xD0\03abc"),
          ('{"U": "abc"}', b"\xD1\x00\03abc"),
          ('{"U": "abc"}', b"\xD2\x00\x00\x00\03abc"),
          ('{"[]": [1, 2]}', b"\x92\x01\x02"),
          ("[1, 2]", b"\xD4\x02\x01\x02"),
          ('{"[]": [1, 2]}', b"\xD5\x00\x02\x01\x02"),
          ('{"[]": [1, 2]}', b"\xD6\x00\x00\x00\x02\x01\x02"),
          ('{"a": "b"}', b"\xA1\x81a\x81b"),
          ('{"a": "b"}', b"\xD8\x01\x81a\x81b"),
          ('{"a": "b"}', b"\xD9\x00\x01\x81a\x81b"),
          ('{"a": "b"}', b"\xDA\x00\x00\x00\x01\x81a\x81b"),
      ))),
    *(
        (version, tag, name, rep, fields)
        for (version, tag, name) in ALL_REQUESTS_PER_VERSION
        for (rep, fields) in (
            (
                ('{"()": [1, [], {}, "e1"]}', b"\xB4\x4E\x01\x90\xA0\x82e1"),
                (
                    '{"->": [1, 2, "a", 3, {}, "e1", "e2", "e3"]}',
                    b"\xB8\x52\x01\x02\x03\x81a\xA0\x82e1\x82e2\x82e3"
                ),
            ) if version >= (5, 0) else (
                ('{"()": [1, [], {}]}', b"\xB3\x4E\x01\x90\xA0"),
                (
                    '{"->": [1, 2, "a", 3, {}]}',
                    b"\xB5\x52\x01\x02\x03\x81a\xA0"
                ),
            )
        )
    ),
    # enough structures tested. translation to Structs and checking
    # equality is covered by unit tests
))  # noqa: PAR102
def test_plays_through(server_version, request_tag, request_name, field_rep,
                       field_bin, server_factory, connection_factory):
    script = parse("""
    !: BOLT {}

    C: {} {}
    S: SUCCESS {{}}
    """.format(".".join(map(str, server_version)), request_name,
               field_rep))

    server = server_factory(script)
    con = connection_factory("localhost", 7687)
    con.write(b"\x60\x60\xb0\x17")
    con.write(server_version_to_version_request(server_version))
    con.read(4)
    msg = b"\xB1" + request_tag + field_bin
    con.write(len(msg).to_bytes(2, "big") + msg + b"\x00\x00")
    res = con.read_message()
    assert res[:2] == b"\xb1\x70"  # SUCCESS
    with pytest.raises(BrokenSocket):
        con.read(1)
    assert not server.service.exceptions


@pytest.mark.parametrize(
    ("server_version",
     "request_tag",
     "request_name",
     "response_tag",
     "response_name"),
    ((v, req_t, req_n, res_t, res_n)
     for v in ALL_BOLT_VERSIONS
     for req_v, req_t, req_n in ALL_REQUESTS_PER_VERSION if req_v == v
     for res_v, res_t, res_n in ALL_RESPONSES_PER_VERSION if res_v == v)
)
@pytest.mark.parametrize("with_auto", (True, False))
def test_manual_replies(server_version, request_tag, request_name,
                        response_tag, response_name, with_auto, server_factory,
                        connection_factory):
    script = parse("""
    !: BOLT {}
    {}
    C: {}
    S: {} {{}}
    """.format(
        ".".join(map(str, server_version)),
        "!: AUTO {}\n".format(request_name) if with_auto else "\n",
        request_name,
        response_name
    ))

    server = server_factory(script)
    con = connection_factory("localhost", 7687)
    con.write(b"\x60\x60\xb0\x17")
    con.write(server_version_to_version_request(server_version))
    con.read(4)
    con.write(b"\x00\x02\xb0" + request_tag + b"\x00\x00")
    res = con.read_message()
    assert res[:2] == b"\xb1" + response_tag
    with pytest.raises(BrokenSocket):
        con.read(1)
    assert not server.service.exceptions


@pytest.mark.parametrize(
    ("server_version", "response_tag", "response_name"),
    (next(r for r in ALL_RESPONSES_PER_VERSION if r[0] == v)
     for v in ALL_BOLT_VERSIONS)
)
def test_initial_response(server_version, response_tag, response_name,
                          server_factory):
    script = parse("""
    !: BOLT {}

    S: {}
    """.format(".".join(map(str, server_version)), response_name))

    server = server_factory(script)
    con = Connection("localhost", 7687)
    con.write(b"\x60\x60\xb0\x17")
    con.write(server_version_to_version_request(server_version))
    con.read(4)
    res = con.read_message()
    assert res[:2] == b"\xb0" + response_tag
    with pytest.raises(BrokenSocket):
        con.read(1)
    assert not server.service.exceptions


@pytest.mark.parametrize("restarting", (False, True))
@pytest.mark.parametrize("concurrent", (False, True))
def test_restarting(server_factory, restarting, concurrent,
                    connection_factory):
    script = """
    !: BOLT 4.3
    {}{}
    S: SUCCESS
    """.format(
        "!: ALLOW RESTART\n" if restarting else "",
        "!: ALLOW CONCURRENT\n" if concurrent else "",
    )

    if restarting and concurrent:
        with pytest.warns(
            Warning, match="concurrent scripts are implicitly restarting"
        ):
            server = server_factory(parse(script))
    else:
        server = server_factory(parse(script))

    for i in range(3):
        if i > 0 and not (restarting or concurrent):
            with pytest.raises((OSError, BrokenSocket)):
                con = connection_factory("localhost", 7687)
                con.write(b"\x60\x60\xb0\x17")
                con.write(server_version_to_version_request((4, 3)))
                con.read(4)
            continue
        con = Connection("localhost", 7687)
        try:
            con.write(b"\x60\x60\xb0\x17")
            con.write(server_version_to_version_request((4, 3)))
            assert con.read(4) == server_version_to_version_response((4, 3))
        finally:
            con.close()
    assert not server.service.exceptions


@pytest.mark.parametrize("restarting", (False, True))
@pytest.mark.parametrize("concurrent", (False, True))
def test_restarting_interwoven(server_factory, restarting, concurrent,
                               connection_factory):
    script = """
    !: BOLT 4.3
    {}{}
    C: HELLO 1
    S: SUCCESS 1
    C: HELLO 2
    S: SUCCESS 2
    """.format(
        "!: ALLOW RESTART\n" if restarting else "",
        "!: ALLOW CONCURRENT\n" if concurrent else "",
    )
    server = server_factory(parse(script))
    for i in range(3):
        if i > 0 and not concurrent:
            with pytest.raises((ConnectionError, OSError, BrokenSocket)):
                con = connection_factory("localhost", 7687)
                con.write(b"\x60\x60\xb0\x17")
                con.write(server_version_to_version_request((4, 3)))
                con.read(4)
            continue
        con = connection_factory("localhost", 7687)
        con.write(b"\x60\x60\xb0\x17")
        con.write(server_version_to_version_request((4, 3)))
        assert con.read(4) == server_version_to_version_response((4, 3))
        con.write(b"\x00\x03")  # 3 byte long message
        con.write(b"\xb1\x01\x01")  # HELLO 1
        con.write(b"\x00\x00")  # end of message
        assert con.read_message() == b"\xb1\x70\x01"  # SUCCESS 1
        # leave connection open!
    assert not server.service.exceptions


@pytest.mark.parametrize("restarting", (False, True))
@pytest.mark.parametrize("concurrent", (False, True))
@pytest.mark.parametrize("msg", (b"\xb0\x11", b"\xb0\x13"))
def test_lists_alternatives_on_unexpected_message(msg, restarting, concurrent,
                                                  server_factory,
                                                  connection_factory):
    script = """
    !: BOLT 4.3
    {}{}
    {{{{
        C: RUN
    ----
        C: RESET
    }}}}
    S: SUCCESS
    """.format(
        "!: ALLOW RESTART\n" if restarting else "",
        "!: ALLOW CONCURRENT\n" if concurrent else "",
    )
    server = server_factory(parse(script))
    con = connection_factory("localhost", 7687)
    con.write(b"\x60\x60\xb0\x17")
    con.write(server_version_to_version_request((4, 3)))
    con.read(4)
    con.write(b"\x00\x02" + msg + b"\x00\x00")
    with pytest.raises(BrokenSocket):
        con.read(6)
    assert len(server.service.exceptions) == 1
    server_exc = server.service.exceptions[0]
    line_offset = restarting + concurrent
    assert "(%3i) C: RUN" % (5 + line_offset) in str(server_exc)
    assert "(%3i) C: RESET" % (7 + line_offset) in str(server_exc)


# @pytest.mark.parametrize("block_marker", ("{?", "{*", "{+", "{{"))
@pytest.mark.parametrize("block_marker", ("{{",))
@pytest.mark.parametrize("msg", (b"\xb0\x11", b"\xb0\x13"))  # BEGIN, ROLLBACK
def test_lists_alternatives_on_unexpected_message_with_non_det_block(
        msg, server_factory, connection_factory, block_marker):
    start_marker = block_marker
    end_marker = "".join(reversed(block_marker.replace("{", "}")))
    script = """
    !: BOLT 4.3

    {}
        C: RUN
    {}
    C: RESET
    S: SUCCESS
    """.format(start_marker, end_marker)
    server = server_factory(parse(script))
    con = connection_factory("localhost", 7687)
    con.write(b"\x60\x60\xb0\x17")
    con.write(server_version_to_version_request((4, 3)))
    con.read(4)
    con.write(b"\x00\x02" + msg + b"\x00\x00")
    with pytest.raises(BrokenSocket):
        con.read(6)
    assert len(server.service.exceptions) == 1
    server_exc = server.service.exceptions[0]
    assert "(  5) C: RUN" in str(server_exc)
    if block_marker in ("{?", "{*"):
        assert "(  7) C: RESET" in str(server_exc)
    else:
        assert "(  7) C: RESET" not in str(server_exc)


def test_unknown_message(server_factory, connection_factory):
    script = """
    !: BOLT 4.3

    C: HELLO
    """
    server = server_factory(parse(script))
    con = connection_factory("localhost", 7687)
    con.write(b"\x60\x60\xb0\x17")
    con.write(server_version_to_version_request((4, 3)))
    con.read(4)
    con.write(b"\x00\x02\xb0\xff\x00\x00")
    with pytest.raises(BrokenSocket):
        con.read(6)
    assert not server.service.exceptions


def test_assert_order_success(server_factory, connection_factory):
    script = """
    !: BOLT 5.3

    C: HELLO
    S: <ASSERT ORDER> 0.5
    S: SUCCESS
    C: GOODBYE
    S: SUCCESS
    """
    server = server_factory(parse(script))
    con = connection_factory("localhost", 7687)
    con.write(b"\x60\x60\xb0\x17")
    con.write(server_version_to_version_request((5, 3)))
    con.read(4)
    t0 = time.monotonic()
    con.write(b"\x00\x02\xb0\x01\x00\x00")  # HELLO
    timeout = con._socket.gettimeout()
    con._socket.settimeout(1)
    msg = con.read_message()
    t1 = time.monotonic()
    con._socket.settimeout(timeout)
    assert t1 - t0 >= 0.5
    assert msg == b"\xb0\x70"  # SUCCESS
    # waited long enough, now send the next message
    con.write(b"\x00\x02\xb0\x02\x00\x00")  # GOODBYE
    msg = con.read_message()
    assert msg == b"\xb0\x70"  # SUCCESS
    assert not server.service.exceptions


def test_assert_order_failure(server_factory, connection_factory):
    script = """
    !: BOLT 5.3

    C: HELLO
    S: <ASSERT ORDER> 1
    S: SUCCESS
    C: GOODBYE
    S: SUCCESS
    """
    server = server_factory(parse(script))
    con = connection_factory("localhost", 7687)
    con.write(b"\x60\x60\xb0\x17")
    con.write(server_version_to_version_request((5, 3)))
    con.read(4)
    con.write(b"\x00\x02\xb0\x01\x00\x00")  # HELLO
    time.sleep(0.5)
    # send something too early while the server still waits to assert order
    con.write(b"\x00\x02\xb0\x02\x00\x00")  # GOODBYE
    time.sleep(1)
    assert len(server.service.exceptions) == 1
    server_exc = server.service.exceptions[0]
    assert isinstance(server_exc, ScriptFailure)
    exc_str = str(server_exc)
    assert ("Expected the driver to not send anything, but received: GOODBYE"
            in exc_str)
    with pytest.raises(BrokenSocket):
        con.read(1)


def test_assert_order_protocol_failure(server_factory, connection_factory):
    script = """
    !: BOLT 5.3

    C: HELLO
    S: <ASSERT ORDER> 1
    S: SUCCESS
    C: GOODBYE
    S: SUCCESS
    """
    server = server_factory(parse(script), address="localhost:7688")
    con = connection_factory("localhost", 7688)
    con.write(b"\x60\x60\xb0\x17")
    con.write(server_version_to_version_request((5, 3)))
    con.read(4)
    con.write(b"\x00\x02\xb0\x01\x00\x00")  # HELLO
    time.sleep(0.5)
    # send something too early while the server still waits to assert order
    con.write(b"\x00\x02\xb0\xff\x00\x00")  # made up message
    time.sleep(1)
    assert len(server.service.exceptions) == 1
    server_exc = server.service.exceptions[0]
    assert isinstance(server_exc, ScriptFailure)
    exc_str = str(server_exc)
    assert "Expected the driver to not send anything, but received" in exc_str
    assert "Unknown response message type FF" in exc_str
    with pytest.raises(BrokenSocket):
        con.read(1)
