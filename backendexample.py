"""
Backend example implemented in python
"""

import sys
import json

from nutkit.backend.backend import decode_hook, Encoder
import nutkit.protocol as protocol


request = ""
in_request = False

_encoder = Encoder()

def handle_request(req):
    if isinstance(req, protocol.NewDriverRequest):
        return protocol.Driver(id="driver1")
    if isinstance(req, protocol.NewSessionRequest):
        return protocol.Session(id="session1")
    if isinstance(req, protocol.SessionRunRequest):
        return protocol.Result(id="result1")
    raise "unknown request"

while True:
    line = sys.stdin.readline()
    if line == "#request begin\n":
        if in_request:
            raise "already in request"
        in_request = True
    elif line == "#request end\n":
        if not in_request:
            raise "end while not in request"

        req = json.loads(request, object_hook=decode_hook)
        res = handle_request(req)
        response = _encoder.encode(res)

        sys.stdout.write("#response begin\n")
        sys.stdout.flush()
        sys.stdout.write(response+"\n")
        sys.stdout.flush()
        sys.stdout.write("#response end\n")
        sys.stdout.flush()
        request = ""
        in_request = False
    else:
        if not in_request:
            raise "line while not in request"
        request = request + line


