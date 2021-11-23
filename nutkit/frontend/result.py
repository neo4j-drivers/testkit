from .. import protocol


class Result:
    def __init__(self, backend, result):
        self._backend = backend
        self._result = result

    def next(self):
        """Move to next record in result."""
        req = protocol.ResultNext(self._result.id)
        return self._backend.send_and_receive(req)

    def single(self):
        """Return one record if there is exactly one.

        Raises error otherwise.
        """
        req = protocol.ResultSingle(self._result.id)
        return self._backend.send_and_receive(req)

    def peek(self):
        """Return the next Record or NullRecord without consuming it."""
        req = protocol.ResultPeek(self._result.id)
        return self._backend.send_and_receive(req)

    def consume(self):
        """Discard all records in result and returns summary."""
        req = protocol.ResultConsume(self._result.id)
        return self._backend.send_and_receive(req)

    def list(self):
        """Retrieve the entire result stream."""
        req = protocol.ResultList(self._result.id)
        res = self._backend.send_and_receive(req)
        assert isinstance(res, protocol.RecordList)
        return res.records

    def keys(self):
        return self._result.keys

    def __iter__(self):
        while True:
            record = self.next()
            if isinstance(record, protocol.NullRecord):
                break
            yield record
