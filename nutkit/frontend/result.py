from .. import protocol


class Result:
    def __init__(self, backend, result):
        self._backend = backend
        self._result = result

    def next(self):
        """ Moves to next record in result.
        """
        req = protocol.ResultNext(self._result.id)
        return self._backend.sendAndReceive(req)

    def consume(self):
        """ Discards all records in result and returns summary.
        """
        req = protocol.ResultConsume(self._result.id)
        return self._backend.sendAndReceive(req)

    def list(self):
        """ Retrieves the entire result stream.
        """
        req = protocol.ResultList(self._result.id)
        return self._backend.sendAndReceive(req)

    def keys(self):
        return self._result.keys

    def __iter__(self):
        while True:
            record = self.next()
            if isinstance(record, protocol.NullRecord):
                break
            yield record
