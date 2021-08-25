from .rx_subscription import RxSubscription
from .. import protocol


class RxSubscriber:
    def __init__(self, backend, subscriber):
        self._backend = backend
        self._subscriber = subscriber

    def get_subscription(self):
        req = protocol.RxGetSubscription(self._subscriber.id)
        res = self._backend.sendAndReceive(req)
        if not isinstance(res, protocol.RxSubscription):
            raise Exception("Should be RxSubscription")
        return RxSubscription(backend=self._backend, subscription=res)

    def next(self):
        req = protocol.RxNext(self._subscriber.id)
        return self._backend.sendAndReceive(req)
