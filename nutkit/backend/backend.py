from contextlib import contextmanager
import inspect
import json
import os
import socket

import nutkit.protocol as protocol

PROTOCOL_CLASSES = dict([
    m for m in inspect.getmembers(protocol, inspect.isclass)])
DEBUG_MESSAGES = os.environ.get("TEST_DEBUG_REQRES", "0").lower() in (
    "1", "y", "yes", "true", "t", "on"
)
DEBUG_TIMEOUT = os.environ.get("TEST_DEBUG_NO_BACKEND_TIMEOUT", "0") in (
    "1", "y", "yes", "true", "t", "on"
)


class Encoder(json.JSONEncoder):
    def default(self, o):
        name = type(o).__name__
        if name in PROTOCOL_CLASSES:
            return {"name": name, "data": o.__dict__}
        return json.JSONEncoder.default(self, o)


def decode_hook(x):
    if "name" not in x:
        return x

    name = x["name"]
    if not isinstance(name, str) or name not in PROTOCOL_CLASSES:
        return x

    data = x.get("data", {})
    if not data:
        data = {}

    return PROTOCOL_CLASSES[name](**data)


# How long to wait before backend responds
DEFAULT_TIMEOUT = None if DEBUG_TIMEOUT else 10


class Backend:
    def __init__(self, address, port):
        self._socket = socket.socket(socket.AF_INET)
        try:
            self._socket.connect((address, port))
        except ConnectionRefusedError:
            try:
                self._socket.close()
            except OSError:
                pass
            raise Exception(
                "Driver backend is not running or is not listening on "
                "port %d or is just refusing connections" % port
            )
        self._encoder = Encoder()
        self._reader = self._socket.makefile(mode="r", encoding="utf-8")
        self._writer = self._socket.makefile(mode="w", encoding="utf-8")
        self.default_timeout = DEFAULT_TIMEOUT

    def close(self):
        self._reader.close()
        self._writer.close()
        self._socket.shutdown(socket.SHUT_RDWR)
        self._socket.close()

    def send(self, req, hooks=None):
        if hooks:
            hook = hooks.get("on_send_" + req.__class__.__name__, None)
            if callable(hook):
                hook(req)
        req_json = self._encoder.encode(req)
        if DEBUG_MESSAGES:
            print("Request: %s" % req_json)
        self._writer.write("#request begin\n")
        self._writer.write(req_json + "\n")
        self._writer.write("#request end\n")
        self._writer.flush()

    def receive(self, timeout=None, hooks=None):
        if timeout is None:
            timeout = self.default_timeout
        self._socket.settimeout(timeout)
        response = ""
        in_response = False
        num_blanks = 0
        while True:
            line = self._reader.readline().strip()
            if line == "#response begin":
                if in_response:
                    raise Exception("already in response")
                in_response = True
            elif line == "#response end":
                if DEBUG_MESSAGES:
                    try:
                        print("Response: %s" % response)
                    except UnicodeEncodeError:
                        print("Response: <invalid unicode>")
                try:
                    res = json.loads(response, object_hook=decode_hook)
                except json.decoder.JSONDecodeError:
                    raise Exception("Failed to decode: %s" % response)

                if hooks:
                    hook = hooks.get("on_receive_" + res.__class__.__name__,
                                     None)
                    if callable(hook):
                        hook(res)
                # All received errors are raised as exceptions
                if isinstance(res, protocol.BaseError):
                    raise res
                return res
            else:
                if in_response:
                    response = response + line
                else:
                    # When backend crashes we will end up reading empty lines
                    # until end of universe.  Use this simple check to detect
                    # this condition and abort
                    if not line:
                        num_blanks += 1
                        if num_blanks > 50:
                            raise Exception(
                                "Detected possible crash in backend"
                            )
                    # The backend can send it's own logs outside of response
                    # blocks
                    elif DEBUG_MESSAGES:
                        print("[BACKEND]: %s" % line)

    def send_and_receive(self, req, timeout=None, hooks=None):
        self.send(req, hooks=hooks)
        return self.receive(timeout, hooks=hooks)


@contextmanager
def backend_timeout_adjustment(backend, timeout):
    old_timeout = backend.default_timeout
    backend.default_timeout = timeout
    yield
    backend.default_timeout = old_timeout
