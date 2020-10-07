import nutkit.protocol as protocol

class Result:
    def __init__(self, backend, result):
        self._backend = backend
        self._result = result

    def next(self):
        """ Moves to next record in result.
        """
        req = protocol.ResultNext(self._result.id)
        return self._backend.sendAndReceive(req)

    def list(self):
        """ Fetches all records in result and returns
        a list of records.
        """
        req = protocol.ResultList(self._result.id)
        return self._backend.sendAndReceive(req)

    def consume(self):
        """ Discards all records in result.
        """
        req = protocol.ResultConsume(self._result.id)
        return self._backend.sendAndReceive(req)
