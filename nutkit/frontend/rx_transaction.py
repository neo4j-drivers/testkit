from .rx_result import RxResult
from .. import protocol


class RxTransaction:
    def __init__(self, backend, id):
        self._backend = backend
        self._id = id

    def run(self, cypher, params=None):
        req = protocol.RxTransactionRun(self._id, cypher, params)
        res = self._backend.sendAndReceive(req)
        if not isinstance(res, protocol.Result):
            raise Exception("Should be Result but was: %s" % res)
        return RxResult(self._backend, res)
