from .rx_publisher import RxPublisher
from .. import protocol


class RxResult:
    def __init__(self, backend, result):
        self._backend = backend
        self._result = result

    def records(self):
        req = protocol.RxRecords(self._result.id)
        res = self._backend.sendAndReceive(req)
        if not isinstance(res, protocol.RxPublisher):
            raise Exception("Should be RxPublisher")
        return RxPublisher(backend=self._backend, publisher=res)
