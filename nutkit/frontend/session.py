from .. import protocol
from ..backend.backend import default_timeout
from .result import Result
from .transaction import Transaction


class ApplicationCodeError(Exception):
    pass


class Session:
    def __init__(self, driver, backend, session):
        self._driver = driver
        self._backend = backend
        self._session = session

    def close(self, hooks=None):
        req = protocol.SessionClose(self._session.id)
        res = self._backend.send_and_receive(req, hooks=hooks)
        if not isinstance(res, protocol.Session):
            raise Exception("Should be session but was: %s" % res)

    def run(self, cypher, params=None, tx_meta=None, timeout=None, hooks=None):
        req = protocol.SessionRun(self._session.id, cypher, params,
                                  txMeta=tx_meta, timeout=timeout)
        self._backend.send(req, hooks=hooks)
        while True:
            res = self._backend.receive(hooks=hooks)
            if isinstance(res, protocol.ResolverResolutionRequired):
                addresses = self._driver.resolve(res.address)
                self._backend.send(
                    protocol.ResolverResolutionCompleted(res.id, addresses),
                    hooks=hooks
                )
            elif isinstance(res, protocol.DomainNameResolutionRequired):
                addresses = self._driver.resolve_domain_name(res.name)
                self._backend.send(
                    protocol.DomainNameResolutionCompleted(res.id, addresses),
                    hooks=hooks
                )
            elif isinstance(res, protocol.Result):
                return Result(self._backend, res)
            else:
                raise Exception(
                    "Should be Result or ResolverResolutionRequired "
                    "but was: %s" % res
                )

    def process_transaction(self, req, fn, config=None, hooks=None):
        self._backend.send(req, hooks=hooks)
        x = None
        while True:
            res = self._backend.receive(timeout=default_timeout, hooks=hooks)
            if isinstance(res, protocol.RetryableTry):
                tx = Transaction(self._backend, res.id)
                try:
                    # Invoke the frontend test function until we succeed, note
                    # that the frontend test function makes calls to the
                    # backend it self.
                    x = fn(tx)
                    # The frontend test function were fine with the
                    # interaction, notify backend that we're happy to go.
                    self._backend.send(
                        protocol.RetryablePositive(self._session.id),
                        hooks=hooks
                    )
                except (ApplicationCodeError, protocol.DriverError) as e:
                    # If this is an error originating from the driver in the
                    # backend, retrieve the id of the error  and send that,
                    # this saves us from having to recreate errors on backend
                    # side, backend just needs to track the returned errors.
                    error_id = ""
                    if isinstance(e, protocol.DriverError):
                        error_id = e.id
                    self._backend.send(
                        protocol.RetryableNegative(self._session.id,
                                                   errorId=error_id),
                        hooks=hooks
                    )
            elif isinstance(res, protocol.ResolverResolutionRequired):
                addresses = self._driver.resolve(res.address)
                self._backend.send(
                    protocol.ResolverResolutionCompleted(res.id, addresses),
                    hooks=hooks
                )
            elif isinstance(res, protocol.DomainNameResolutionRequired):
                addresses = self._driver.resolve_domain_name(res.name)
                self._backend.send(
                    protocol.DomainNameResolutionCompleted(res.id, addresses),
                    hooks=hooks
                )
            elif isinstance(res, protocol.RetryableDone):
                return x

    def read_transaction(self, fn, tx_meta=None, timeout=None, hooks=None):
        # Send request to enter transactional read function
        req = protocol.SessionReadTransaction(
            self._session.id, txMeta=tx_meta, timeout=timeout
        )
        return self.process_transaction(req, fn, hooks=hooks)

    def write_transaction(self, fn, tx_meta=None, timeout=None, hooks=None):
        # Send request to enter transactional read function
        req = protocol.SessionWriteTransaction(
            self._session.id, txMeta=tx_meta, timeout=timeout
        )
        return self.process_transaction(req, fn, hooks=hooks)

    def begin_transaction(self, tx_meta=None, timeout=None, hooks=None):
        req = protocol.SessionBeginTransaction(
            self._session.id, txMeta=tx_meta, timeout=timeout
        )
        res = self._backend.send_and_receive(req, hooks=hooks)
        if not isinstance(res, protocol.Transaction):
            raise Exception("Should be Transaction but was: %s" % res)
        return Transaction(self._backend, res.id)

    def last_bookmarks(self, hooks=None):
        req = protocol.SessionLastBookmarks(self._session.id)
        res = self._backend.send_and_receive(req, hooks=hooks)
        if not isinstance(res, protocol.Bookmarks):
            raise Exception("Should be Bookmarks but was: %s" % res)
        return res.bookmarks
