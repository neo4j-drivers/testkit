import nutkit.protocol as protocol
from .result import Result
from .transaction import Transaction


class Session:
    def __init__(self, backend, session):
        self._backend = backend
        self._session = session

    def close(self):
        req = protocol.SessionClose(self._session.id)
        res = self._backend.sendAndReceive(req)
        if not isinstance(res, protocol.Session):
            raise "Should be session"

    def run(self, cypher, params=None, txMeta=None, timeout=None):
        req = protocol.SessionRun(self._session.id, cypher, params, txMeta=txMeta, timeout=timeout)
        res = self._backend.sendAndReceive(req)
        if not isinstance(res, protocol.Result):
            raise "Should be result"
        return Result(self._backend, res)

    def processTransaction(self, req, fn, config=None):
        self._backend.send(req)
        x = None
        while True:
            res = self._backend.receive()
            if isinstance(res, protocol.RetryableTry):
                tx = Transaction(self._backend, res.id)
                try:
                    # Invoke the frontend test function until we succeed, note that the
                    # frontend test function makes calls to the backend it self.
                    x = fn(tx)
                    # The frontend test function were fine with the interaction, notify backend
                    # that we're happy to go.
                    self._backend.send(protocol.RetryablePositive(self._session.id))
                except Exception as e:
                    # If the backend failed to handle this, raise for easier debugging
                    if isinstance(e, protocol.BackendError):
                        raise e

                    # If this is an error originating from the driver in the backend, retrieve the
                    # id of the error  and send that, this saves us from having to recreate errors
                    # on backend side, backend just needs to track the returned errors.
                    errorId = ""
                    if isinstance(e, protocol.DriverError):
                        errorId = e.id
                    self._backend.send(protocol.RetryableNegative(self._session.id, errorId=errorId))
            elif isinstance(res, protocol.RetryableDone):
                return x

    def readTransaction(self, fn, txMeta=None, timeout=None):
        # Send request to enter transactional read function
        req = protocol.SessionReadTransaction(self._session.id, txMeta=txMeta, timeout=timeout)
        return self.processTransaction(req, fn)

    def writeTransaction(self, fn, txMeta=None, timeout=None):
        # Send request to enter transactional read function
        req = protocol.SessionWriteTransaction(self._session.id, txMeta=txMeta, timeout=timeout)
        return self.processTransaction(req, fn)

    def beginTransaction(self, txMeta=None, timeout=None):
        req = protocol.SessionBeginTransaction(self._session.id, txMeta=txMeta, timeout=timeout)
        res = self._backend.sendAndReceive(req)
        if not isinstance(res, protocol.Transaction):
            raise "Should be transaction"
        return Transaction(self._backend, res.id)


