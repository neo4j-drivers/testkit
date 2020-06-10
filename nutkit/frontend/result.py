import nutkit.protocol as protocol

class Result:
    def __init__(self, backend, result):
        self._backend = backend
        self._result = result

    def next(self):
        req = protocol.ResultNext(self._result.id)
        return self._backend.sendAndReceive(req)
