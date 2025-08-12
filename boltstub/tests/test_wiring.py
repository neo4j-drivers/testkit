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


from functools import reduce
from random import getrandbits

import pytest

from ..wiring import (
    create_wire,
    negotiate_socket,
    RegularSocket,
    WebSocket,
)


def randbytes(size):
    return bytearray(getrandbits(8) for _ in range(size))


class TestRegularSocket:
    class TestWhenCacheIsSupplied:

        def test_recv_should_return_cache(self, mocker):
            cache = b"abc"

            socket_mock = mocker.Mock()
            regular_socket = RegularSocket(socket_mock, cache)

            received = regular_socket.recv(1024)

            assert received == cache
            socket_mock.recv.assert_not_called()

        def test_accessing_the_socket_during_second_call(self, mocker):
            cache = b"abc"
            expected = b"barcelos"

            socket_mock = mocker.Mock()
            socket_mock.recv.return_value = expected
            regular_socket = RegularSocket(socket_mock, cache)

            regular_socket.recv(1024)
            actual = regular_socket.recv(1024)

            assert actual == expected
            socket_mock.recv.assert_called_with(1024)

    class TestWhenCacheIsEmpty:

        @pytest.mark.parametrize("empty_cache", [
            None,
            b"",
        ])
        def test_accessing_the_socket(self, empty_cache, mocker):
            expected = b"barcelos"

            socket_mock = mocker.Mock()
            socket_mock.recv.return_value = expected
            regular_socket = RegularSocket(socket_mock, empty_cache)

            actual = regular_socket.recv(1024)

            assert actual == expected
            socket_mock.recv.assert_called_with(1024)

    @pytest.mark.parametrize("method_name", [
        "close",
        "send",
        "sendall",
        "settimeout",
        "getsockname",
        "getpeername",
    ])
    def test_proxying_sockets_members(self, method_name, mocker):
        socket_mock = mocker.Mock()
        regular_socket = RegularSocket(socket_mock, None)

        wrapped_method = getattr(regular_socket, method_name)
        original_method = getattr(socket_mock, method_name)

        assert wrapped_method == original_method


class TestWebSocket:

    @pytest.mark.parametrize("payload,frame", [
        (randbytes(0), b"\x82\x00"),
        (randbytes(7), b"\x82\x07"),
        (randbytes(125), b"\x82\x7D"),
        (randbytes(126), b"\x82\x7E\x00\x7E"),
        (randbytes(65535), b"\x82\x7E\xFF\xFF"),
        (randbytes(65536), b"\x82\x7F\x00\x00\x00\x00\x00\x01\x00\x00"),
        (randbytes(65537), b"\x82\x7F\x00\x00\x00\x00\x00\x01\x00\x01"),
    ])
    def test_send(self, payload, frame, mocker):
        socket_mock = mocker.Mock()
        websocket = WebSocket(socket_mock)

        bytes_sent = websocket.send(payload)

        assert len(payload) == bytes_sent
        socket_mock.sendall.assert_called_once()
        socket_mock.sendall.assert_called_with(frame + payload)

    @pytest.mark.parametrize("payload,frame", [
        (randbytes(0), b"\x82\x00"),
        (randbytes(7), b"\x82\x07"),
        (randbytes(125), b"\x82\x7D"),
        (randbytes(126), b"\x82\x7E\x00\x7E"),
        (randbytes(65535), b"\x82\x7E\xFF\xFF"),
        (randbytes(65536), b"\x82\x7F\x00\x00\x00\x00\x00\x01\x00\x00"),
        (randbytes(65537), b"\x82\x7F\x00\x00\x00\x00\x00\x01\x00\x01"),
    ])
    def test_sendall(self, payload, frame, mocker):
        socket_mock = mocker.Mock()
        websocket = WebSocket(socket_mock)

        websocket.sendall(payload)

        socket_mock.sendall.assert_called_once()
        socket_mock.sendall.assert_called_with(frame + payload)

    @pytest.mark.parametrize("payload,frame", [
        (randbytes(0), b"\x82\x00"),
        (randbytes(7), b"\x82\x07"),
        (randbytes(125), b"\x82\x7D"),
        (randbytes(126), b"\x82\x7E\x00\x7E"),
        (randbytes(65535), b"\x82\x7E\xFF\xFF"),
        (randbytes(65536), b"\x82\x7F\x00\x00\x00\x00\x00\x01\x00\x00"),
        (randbytes(65537), b"\x82\x7F\x00\x00\x00\x00\x00\x01\x00\x01"),
    ])
    def test_recv(self, payload, frame, mocker):
        socket_mock = mocker.Mock()
        framed_payload = frame + payload
        socket_mock.recv.side_effect = TestWebSocket.mock_recv(framed_payload)

        websocket = WebSocket(socket_mock)

        received_payload = websocket.recv(1234)

        assert received_payload == payload

    @pytest.mark.parametrize("payload,frame", [
        (randbytes(0), b"\x82\x00"),
        (randbytes(7), b"\x82\x07"),
        (randbytes(125), b"\x82\x7D"),
        (randbytes(126), b"\x82\x7E\x00\x7E"),
        (randbytes(65535), b"\x82\x7E\xFF\xFF"),
        (randbytes(65536), b"\x82\x7F\x00\x00\x00\x00\x00\x01\x00\x00"),
        (randbytes(65537), b"\x82\x7F\x00\x00\x00\x00\x00\x01\x00\x01"),
    ])
    def test_recv_pong(self, payload, frame, mocker):
        socket_mock = mocker.Mock()
        pong = b"\x0A\x00"
        framed_payload = pong + frame + payload
        socket_mock.recv.side_effect = TestWebSocket.mock_recv(framed_payload)

        websocket = WebSocket(socket_mock)

        received_payload = websocket.recv(1234)

        assert received_payload == payload

    @pytest.mark.parametrize("payload,frame", [
        (randbytes(0), b"\x82\x00"),
        (randbytes(7), b"\x82\x07"),
        (randbytes(125), b"\x82\x7D"),
        (randbytes(126), b"\x82\x7E\x00\x7E"),
        (randbytes(65535), b"\x82\x7E\xFF\xFF"),
        (randbytes(65536), b"\x82\x7F\x00\x00\x00\x00\x00\x01\x00\x00"),
        (randbytes(65537), b"\x82\x7F\x00\x00\x00\x00\x00\x01\x00\x01"),
    ])
    def test_recv_ping(self, payload, frame, mocker):
        socket_mock = mocker.Mock()
        ping = b"\x09\x00"
        pong = b"\x0A\x00"
        framed_payload = ping + frame + payload
        socket_mock.recv.side_effect = TestWebSocket.mock_recv(framed_payload)

        websocket = WebSocket(socket_mock)

        received_payload = websocket.recv(1234)

        assert received_payload == payload
        socket_mock.sendall.assert_called_once()
        socket_mock.sendall.assert_called_with(pong)

    @pytest.mark.parametrize("reserved_control_frame", [
        b"\x0B\x00",
        b"\x0C\x00",
        b"\x0D\x00",
        b"\x0E\x00",
        b"\x0F\x00",
    ])
    def test_recv_reserved_control_frame(self, reserved_control_frame, mocker):
        socket_mock = mocker.Mock()
        frame = b"\x82\x7E\x00\x7E"
        payload = randbytes(126)
        framed_payload = reserved_control_frame + frame + payload
        socket_mock.recv.side_effect = TestWebSocket.mock_recv(framed_payload)

        websocket = WebSocket(socket_mock)

        received_payload = websocket.recv(1234)

        assert received_payload == payload

    def test_recv_masked_frame(self, mocker):
        payload = bytearray(125)
        mask = b"\x0A\x0B\x0C\x0C"
        frame = b"\x82\xFD" + mask
        masked_payload = bytearray(payload[i] ^ mask[i % 4]
                                   for i in range(125))

        framed_payload = frame + masked_payload

        socket_mock = mocker.Mock()
        socket_mock.recv.side_effect = TestWebSocket.mock_recv(framed_payload)

        websocket = WebSocket(socket_mock)

        received_payload = websocket.recv(1234)

        assert received_payload == payload

    def test_recv_multiple_frames(self, mocker):
        payload_list = [
            (randbytes(0), b"\x02\x00"),
            (randbytes(7), b"\x00\x07"),
            (randbytes(125), b"\x00\x7D"),
            (randbytes(126), b"\x00\x7E\x00\x7E"),
            (randbytes(65535), b"\x00\x7E\xFF\xFF"),
            (randbytes(65536), b"\x00\x7F\x00\x00\x00\x00\x00\x01\x00\x00"),
            (randbytes(65537), b"\x82\x7F\x00\x00\x00\x00\x00\x01\x00\x01")
        ]
        framed_payload = reduce(lambda x, y: x + y,
                                map(lambda p: p[1] + p[0], payload_list))
        full_payload = reduce(lambda x, y: x + y,
                              map(lambda p: p[0], payload_list))

        socket_mock = mocker.Mock()
        socket_mock.recv.side_effect = TestWebSocket.mock_recv(framed_payload)

        websocket = WebSocket(socket_mock)

        received_payload = websocket.recv(1234)

        assert received_payload == full_payload

    @pytest.mark.parametrize("method_name", [
        "close",
        "settimeout",
        "getsockname",
        "getpeername",
    ])
    def test_proxying_sockets_members(self, method_name, mocker):
        socket_mock = mocker.Mock()
        regular_socket = WebSocket(socket_mock)

        wrapped_method = getattr(regular_socket, method_name)
        original_method = getattr(socket_mock, method_name)

        assert wrapped_method == original_method

    @staticmethod
    def mock_recv(payload_):
        state = {"pos": 0}

        def recv(*args, **kwargs):
            size = args[0]
            start = state.get("pos")
            end = start + size
            state["pos"] = end
            return payload_[start:end]
        return recv


def test_create_wire(mocker):
    socket = mocker.Mock()
    read_wake_up = False
    wrap_socket = mocker.Mock()

    regular_socket = RegularSocket(socket, None)
    wrap_socket.return_value = regular_socket

    wire = create_wire(socket, read_wake_up, wrap_socket)

    assert wire._socket == regular_socket
    wrap_socket.assert_called_with(socket)


def test_negotiate_socket_negotiate_regular_socket(mocker):
    payload = (b"\x60\x60\xB0\x17\x00\x00\x01\x04\x00\x00\x00\x04\x00\x00\x00"
               b"\x03\x00\x00\x00\x00")
    socket = mocker.Mock()
    socket.recv.return_value = payload

    negotiated_socket = negotiate_socket(socket)

    assert isinstance(negotiated_socket,  RegularSocket)
    assert negotiated_socket._socket == socket
    assert negotiated_socket._cache == payload
    socket.recv.assert_called_once()


def test_negotiate_socket_negotiate_web_socket(mocker):
    payload_text = ("GET /chat HTTP/1.1\r\n"
                    + "Host: server.example.com\r\n"
                    + "Upgrade: websocket\r\n"
                    + "Connection: Upgrade\r\n"
                    + "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n"
                    + "Origin: http://example.com\r\n"
                    + "Sec-WebSocket-Protocol: chat, superchat\r\n"
                    + "Sec-WebSocket-Version: 13\r\n\r\n")
    payload = payload_text.encode("utf-8")

    response_payload_text = (
        "HTTP/1.1 101 Switching Protocols\r\n"
        + "Upgrade: websocket\r\n"
        + "Connection: Upgrade\r\n"
        + "Sec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\r\n\r\n"
    )

    socket = mocker.Mock()
    socket.recv.return_value = payload

    negotiated_socket = negotiate_socket(socket)

    assert isinstance(negotiated_socket,  WebSocket)
    assert negotiated_socket._socket == socket
    socket.recv.assert_called_once()
    socket.sendall.assert_called_once()

    encoded_sent_response, = socket.sendall.call_args.args
    decoded_sent_response = encoded_sent_response.decode("utf-8")
    assert decoded_sent_response == response_payload_text
