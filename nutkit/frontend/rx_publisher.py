from .rx_subscriber import RxSubscriber
from .. import protocol


class RxPublisher:
    def __init__(self, backend, publisher):
        self._backend = backend
        self._publisher = publisher

    def subscribe(self):
        req = protocol.RxSubscribe(self._publisher.id)
        res = self._backend.sendAndReceive(req)
        if not isinstance(res, protocol.RxSubscriber):
            raise Exception("Should be RxSubscriber")
        return RxSubscriber(backend=self._backend, subscriber=res)
