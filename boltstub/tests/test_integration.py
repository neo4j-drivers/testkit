import pytest
import threading
import socket
from struct import unpack as struct_unpack
import traceback

from .. import BoltStubService
from ..parsing import parse
from ..util import hex_repr


ALL_SERVER_VERSIONS = (1,), (2,), (3,), (4, 1), (4, 2), (4, 3)


ALL_REQUESTS_PER_VERSION = tuple((
    *(((major,), tag, name)
      for major in (1, 2)
      for (tag, name) in (
          (b"\x01", "INIT"),
          (b"\x0E", "ACK_FAILURE"),
          (b"\x0F", "RESET"),
          (b"\x10", "RUN"),
          (b"\x2F", "DISCARD_ALL"),
          (b"\x3F", "PULL_ALL"),
      )),

    ((3,), b"\x01", "HELLO"),
    ((3,), b"\x02", "GOODBYE"),
    ((3,), b"\x0F", "RESET"),
    ((3,), b"\x10", "RUN"),
    ((3,), b"\x2F", "DISCARD_ALL"),
    ((3,), b"\x3F", "PULL_ALL"),
    ((3,), b"\x11", "BEGIN"),
    ((3,), b"\x12", "COMMIT"),
    ((3,), b"\x13", "ROLLBACK"),

    *(((4, minor), tag, name)
      for minor in range(4)
      for (tag, name) in (
          (b"\x01", "HELLO"),
          (b"\x02", "GOODBYE"),
          (b"\x0F", "RESET"),
          (b"\x10", "RUN"),
          (b"\x2F", "DISCARD"),
          (b"\x3F", "PULL"),
          (b"\x11", "BEGIN"),
          (b"\x12", "COMMIT"),
          (b"\x13", "ROLLBACK"),
      ))
))


ALL_RESPONSES_PER_VERSION = tuple((
    *((version, tag, name)
      for version in ALL_SERVER_VERSIONS
      for (tag, name) in (
          (b"\x70", "SUCCESS"),
          (b"\x7E", "IGNORED"),
          (b"\x7F", "FAILURE"),
          (b"\x71", "RECORD"),
      )),
))


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
            except (ConnectionResetError, ) as e:
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


class ThreadedServer(threading.Thread):
    def __init__(self, script, address):
        super().__init__(daemon=True)
        if isinstance(script, str):
            script = parse(script)
        self.service = BoltStubService(script, address, timeout=1)
        self.exc = None

    def run(self):
        try:
            self.service.start()
        except BaseException as e:
            traceback.print_exc()
            self.exc = e

    def stop(self):
        self.service.stop()

    def join(self, timeout=None):
        super().join(timeout=timeout)


@pytest.fixture()
def server_factory():
    server = None

    def factory(script, address="localhost:7687"):
        nonlocal server
        server = ThreadedServer(script, address)
        server.start()
        return server

    yield factory
    if server is not None:
        server.stop()
        server.join()


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


@pytest.mark.parametrize("server_version", ALL_SERVER_VERSIONS)
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

    C: NOMATCH
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


@pytest.mark.parametrize(("client_version", "server_version", "matches"), [
    [b"\x00\x00\x00\x01", (1,), True],
    [b"\x00\x00\x00\x01", (1, 0), True],
    [b"\x00\x00\x00\x01\x00\x00\x00\x02", (1, 0), True],
    [b"\x00\x00\x00\x01\x00\x00\x00\x02", (2, 0), True],
    [b"\x00\x00\x00\x01\x00\x00\x00\x03", (2,), False],
    [b"\x00\x00\x00\x03", (3,), True],
    [b"\x00\x00\x00\x04", (4,), True],
    [b"\x00\x00\x01\x04", (4, 1), True],
    [b"\x00\x00\x02\x04", (4, 2), True],
    [b"\x00\x00\x03\x04\x00\x00\x02\x04\x00\x00\x01\x04", (4, 2), True],
    [b"\x00\x00\x03\x04", (4, 3), True],
    [b"\x00\x02\x03\x04", (4, 3), True],
    [b"\x00\x03\x03\x04\x00\x00\x02\x04\x00\x00\x01\x04\x00\x00\x00\x03",
     (4, 0), True],
    # ignore minor versions until 4.0
    [b"\x00\x00\x10\x01", (1,), True],
    [b"\x00\x00\x10\x02", (2,), True],
    [b"\x00\x00\x10\x03", (3,), True],
    [b"\x00\x00\x10\x04", (4, 0), False],
    [b"\x00\x00\x10\x04", (4, 1), False],
    [b"\x00\x00\x10\x04", (4, 2), False],
    [b"\x00\x00\x10\x04", (4, 3), False],
    # ignore version ranges until 4.0
    [b"\x00\x09\x0A\x01", (1,), True],
    [b"\x00\x0A\x0A\x01", (1,), True],
    [b"\x00\x09\x0A\x02", (2,), True],
    [b"\x00\x0A\x0A\x02", (2,), True],
    [b"\x00\x09\x0A\x03", (3,), True],
    [b"\x00\x0A\x0A\x03", (3,), True],
    [b"\x00\x0A\x0A\x04", (4, 0), True],
    [b"\x00\x09\x0A\x04", (4, 0), False],
    [b"\x00\x0A\x0A\x04", (4, 1), True],
    [b"\x00\x09\x0A\x04", (4, 1), True],
    [b"\x00\x08\x0A\x04", (4, 1), False],
    [b"\x00\x09\x0A\x04", (4, 2), True],
    [b"\x00\x08\x0A\x04", (4, 2), True],
    [b"\x00\x07\x0A\x04", (4, 2), False],
    [b"\x00\x08\x0A\x04", (4, 3), True],
    [b"\x00\x07\x0A\x04", (4, 3), True],
    [b"\x00\x06\x0A\x04", (4, 3), False],
])
def test_handshake_auto(client_version, server_version, matches,
                        server_factory, connection_factory):
    client_version = client_version + b"\x00" * (16 - len(client_version))

    script = parse("""
    !: BOLT {}
    
    C: NOMATCH
    """.format(".".join(map(str, server_version))))

    server = server_factory(script)
    con = connection_factory("localhost", 7687)
    con.write(b"\x60\x60\xb0\x17")
    con.write(client_version)
    if matches:
        assert con.read(4) == server_version_to_version_response(server_version)
    else:
        assert con.read(4) == b"\x00" * 4
        with pytest.raises(BrokenSocket):
            print(con.read(1))
    assert not server.service.exceptions


@pytest.mark.parametrize("custom_handshake", [b"\x00\x00\xFF\x00", b"foobar"])
@pytest.mark.parametrize("client_version", [b"\x00\x00\x00\x01", b"crap"])
@pytest.mark.parametrize("server_version", ALL_SERVER_VERSIONS)
def test_custom_handshake_auto(custom_handshake, client_version, server_version,
                               server_factory, connection_factory):
    client_version = client_version + b"\x00" * (16 - len(client_version))

    script = parse("""
    !: BOLT {}
    !: HANDSHAKE {}

    C: NOMATCH
    """.format(".".join(map(str, server_version)), hex_repr(custom_handshake)))

    server = server_factory(script)
    con = connection_factory("localhost", 7687)
    con.write(b"\x60\x60\xb0\x17")
    con.write(client_version)
    assert con.read(len(custom_handshake)) == custom_handshake
    with pytest.raises(socket.timeout):
        con.read(1)
    assert not server.service.exceptions


@pytest.mark.parametrize(("server_version", "request_tag", "request_name"),
                         ALL_REQUESTS_PER_VERSION)
def test_auto_replies(server_version, request_tag, request_name,
                      server_factory, connection_factory):
    script = parse("""
    !: BOLT {}
    !: AUTO {}

    C: NOMATCH
    """.format(".".join(map(str, server_version)), request_name))

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


@pytest.mark.parametrize(
    ("server_version",
     "request_tag",
     "request_name",
     "response_tag",
     "response_name"),
    ((v, req_t, req_n, res_t, res_n)
     for v in ALL_SERVER_VERSIONS
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
        response_name)
    )

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
     for v in ALL_SERVER_VERSIONS)
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
    with pytest.raises(socket.timeout):
        con.read(1)
    assert not server.service.exceptions


@pytest.mark.parametrize("restarting", (False, True))
@pytest.mark.parametrize("concurrent", (False, True))
def test_restarting(server_factory, restarting, concurrent, connection_factory):
    script = """
    !: BOLT 4.3
    {}{}
    S: SUCCESS
    """.format(
        "!: ALLOW RESTART\n" if restarting else "",
        "!: ALLOW CONCURRENT\n" if concurrent else "",
    )

    server = server_factory(parse(script))
    for i in range(3):
        if i > 0 and not (restarting or concurrent):
            with pytest.raises((ConnectionError, OSError, BrokenSocket)):
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


@pytest.mark.parametrize("msg", (b"\xb0\x11", b"\xb0\x13"))
def test_lists_alternatives_on_unexpected_message(msg, server_factory,
                                                  connection_factory):
    script = """
    !: BOLT 4.3
    
    {{
        C: RUN
    ----
        C: RESET
    }}
    S: SUCCESS
    """
    server = server_factory(parse(script))
    con = connection_factory("localhost", 7687)
    con.write(b"\x60\x60\xb0\x17")
    con.write(server_version_to_version_request((4, 3)))
    con.read(4)
    con.write(b"\x00\x02" + msg + b"\x00\x00")
    with pytest.raises(BrokenSocket):
        con.read(6)
    assert len(server.service.exceptions) == 1
    assert "(5)C: RUN" in str(server.service.exceptions[0])
    assert "(7)C: RESET" in str(server.service.exceptions[0])


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
