from subprocess import Popen, PIPE
from select import select
import json
import sys
import inspect
import threading

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
    def __init__(self, args):
        self._p = Popen(
            args, bufsize=0, encoding='utf-8',
            stdin=PIPE, stdout=PIPE, stderr=PIPE, shell=False)
        self._encoder = Encoder()
        self._timeout = False
        self._died = False

    def _onTimeout(self):
        self._died = self._p.poll() != None
        self._p.kill()
        self._timeout = True

    def sendAndReceive(self, req, timeout=2):
        reqJson = self._encoder.encode(req)
        self._p.stdin.write("#request begin\n")
        self._p.stdin.write(reqJson+"\n")
        self._p.stdin.write("#request end\n")

        response = ""
        in_response = False
        while True:
            t = threading.Timer(timeout, lambda: self._onTimeout())
            t.start()
            line = self._p.stdout.readline()
            t.cancel()
            if self._timeout:
                raise "timeout"
            if line == "" or self._died:
                if self._p.poll() != None:
                    if self._p.returncode != 0:
                        sys.stdout.write(">>> Error output from subprocess:\n")
                        for l in self._p.stderr:
                            sys.stdout.write(l)
                        sys.stdout.write("<<< End error output from subprocess\n\n")
                        sys.stdout.flush()
                        raise "died"
                    else:
                        raise "exited"
                else:
                    sys.stdout.write(line)
            if line == "#response begin\n":
                if in_response:
                    raise "already in response"
                in_response = True
            elif line == "#response end\n":
                res = json.loads(response, object_hook=decode_hook)
                return res
            else:
                if in_response:
                    response = response + line
                else:
                    sys.stdout.write(line)

