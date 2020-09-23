import json
import sys
import inspect
import threading
import socket
import os

import nutkit.protocol as protocol

protocolClasses = dict([m for m in inspect.getmembers(protocol, inspect.isclass)])
debug = os.environ.get('TEST_DEBUG_REQRES', 0)

class Encoder(json.JSONEncoder):
    def default(self, o):
        name = type(o).__name__
        if name in protocolClasses:
            return {"name":name, "data":o.__dict__ }
        return json.JSONEncoder.default(self, o)


def decode_hook(x):
    if not 'name' in x:
        return x

    name = x['name']
    if not name in protocolClasses:
        return x

    data = x['data']
    if not data:
        data = {}

    return protocolClasses[name](**data)


# How long to wait before backend responds
default_timeout = 10


class Backend:
    def __init__(self, address, port):
        self._socket = socket.socket(socket.AF_INET)
        try:
            self._socket.connect((address, port))
        except ConnectionRefusedError:
            raise Exception("Driver backend is not running or is not listening on port %d or is just refusing connections" % port)
        self._encoder = Encoder()
        self._reader = self._socket.makefile(mode='r', encoding='utf-8')
        self._writer = self._socket.makefile(mode='w', encoding='utf-8')

    def close(self):
        self._socket.shutdown(socket.SHUT_RDWR)
        self._socket.close()

    def send(self, req):
        reqJson = self._encoder.encode(req)
        if debug:
            print("Request: %s" % reqJson)
        self._writer.write("#request begin\n")
        self._writer.write(reqJson+"\n")
        self._writer.write("#request end\n")
        self._writer.flush()

    def receive(self, timeout=default_timeout):
        self._socket.settimeout(timeout)
        response = ""
        in_response = False
        num_blanks = 0
        while True:
            line = self._reader.readline().strip()
            if line == "#response begin":
                if in_response:
                    raise "already in response"
                in_response = True
            elif line == "#response end":
                if debug:
                    try:
                        print("Response: %s" % response)
                    except UnicodeEncodeError:
                        print("Response: <invalid unicode>")
                try:
                    res = json.loads(response, object_hook=decode_hook)
                except json.decoder.JSONDecodeError as e:
                    raise Exception("Failed to decode: %s" % response)

                # All received errors are raised as exceptions
                if isinstance(res, protocol.BaseError):
                    raise res
                return res
            else:
                if in_response:
                    response = response + line
                else:
                    # When backend crashes we will end up reading empty lines until end of universe
                    # Use this simple check to detect this condition and abort
                    if not line:
                        num_blanks += 1
                        if num_blanks > 50:
                            raise Exception("Detected possible crash in backend")
                    # The backend can send it's own logs outside of response blocks
                    elif debug:
                        print("[BACKEND]: %s" % line)

    def sendAndReceive(self, req, timeout=default_timeout):
        self.send(req)
        return self.receive(timeout)

