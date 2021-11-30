
import pytest

from ..wiring import RegularSocket


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

            _ = regular_socket.recv(1024)
            actual = regular_socket.recv(1024)

            assert actual == expected
            socket_mock.recv.assert_called_with(1024)

    class TestWhenCacheIsEmpty:

        @pytest.mark.parametrize("empty_cache", [
            (None),
            (b"")
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
        ("close"),
        ("send"),
        ("sendall"),
        ("settimeout"),
        ("getsockname"),
        ("getpeername")
    ])
    def test_proxying_sockets_members(self, method_name, mocker):
        socket_mock = mocker.Mock()
        regular_socket = RegularSocket(socket_mock, None)

        wrapped_method = getattr(regular_socket, method_name)
        original_method = getattr(socket_mock, method_name)

        assert wrapped_method == original_method
