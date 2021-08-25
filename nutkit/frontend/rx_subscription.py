from .. import protocol


class RxSubscription:
    def __init__(self, backend, subscription):
        self._backend = backend
        self._subscription = subscription

    def request(self, n):
        req = protocol.RxSubscriptionRequest(self._subscription.id, n=n)
        res = self._backend.sendAndReceive(req)
        if not isinstance(res, protocol.RxSubscriber):
            raise Exception("Should be RxSubscriber")
        return res
