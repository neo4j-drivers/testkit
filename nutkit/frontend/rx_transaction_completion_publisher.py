from .rx_transaction import RxTransaction
from .. import protocol
from ..backend.backend import default_timeout


class ApplicationCodeException(Exception):
    pass


class RxTransactionCompletionPublisher:
    def __init__(self, driver, backend, session, work_fn):
        self._driver = driver
        self._backend = backend
        self._session = session
        self._work_fn = work_fn

    def subscribe_and_consume(self):
        req = protocol.RxExecuteTransaction(self._session.id)
        self._backend.send(req)
        x = None
        while True:
            res = self._backend.receive(timeout=default_timeout)
            if isinstance(res, protocol.RetryableTry):
                tx = RxTransaction(self._backend, res.id)
                try:
                    # Invoke the frontend test function until we succeed, note
                    # that the frontend test function makes calls to the
                    # backend it self.
                    x = self._work_fn(tx)
                    # The frontend test function were fine with the
                    # interaction, notify backend that we're happy to go.
                    self._backend.send(
                        protocol.RetryablePositive(self._session.id)
                    )
                except (ApplicationCodeException, protocol.DriverError) as e:
                    # If this is an error originating from the driver in the
                    # backend, retrieve the id of the error  and send that,
                    # this saves us from having to recreate errors on backend
                    # side, backend just needs to track the returned errors.
                    errorId = ""
                    if isinstance(e, protocol.DriverError):
                        errorId = e.id
                    self._backend.send(
                        protocol.RetryableNegative(self._session.id,
                                                   errorId=errorId)
                    )
            elif isinstance(res, protocol.ResolverResolutionRequired):
                addresses = self._driver.resolve(res.address)
                self._backend.send(
                    protocol.ResolverResolutionCompleted(res.id, addresses)
                )
            elif isinstance(res, protocol.DomainNameResolutionRequired):
                addresses = self._driver.resolveDomainName(res.name)
                self._backend.send(
                    protocol.DomainNameResolutionCompleted(res.id, addresses)
                )
            elif isinstance(res, protocol.RetryableDone):
                return x
