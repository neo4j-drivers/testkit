import json
import sys
import inspect
import threading
import socket

import nutkit.protocol as protocol

protocolClasses = dict([m for m in inspect.getmembers(protocol, inspect.isclass)])

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


class Backend:
    def __init__(self, address, port):
        self._socket = socket.socket(socket.AF_INET)
        self._socket.connect((address, port))
        self._encoder = Encoder()
        self._reader = self._socket.makefile(mode='r', encoding='utf-8')
        self._writer = self._socket.makefile(mode='w', encoding='utf-8')

    def close(self):
        self._socket.close()

    def send(self, req):
        reqJson = self._encoder.encode(req)
        self._writer.write("#request begin\n")
        self._writer.write(reqJson+"\n")
        self._writer.write("#request end\n")
        self._writer.flush()

    def receive(self, timeout=10):
        self._socket.settimeout(timeout)
        response = ""
        in_response = False
        while True:
            line = self._reader.readline().strip()
            if line == "#response begin":
                if in_response:
                    raise "already in response"
                in_response = True
            elif line == "#response end":
                res = json.loads(response, object_hook=decode_hook)
                # All received errors are raised as exceptions
                if isinstance(res, protocol.BaseError):
                    raise res
                return res
            else:
                if in_response:
                    response = response + line
                else:
                    sys.stdout.write(line)

    def sendAndReceive(self, req, timeout=10):
        self.send(req)
        return self.receive(timeout)

