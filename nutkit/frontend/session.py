from .. import protocol
from ._transaction_loop import (
    handle_retry_func,
    run_tx_loop,
)
from .eager_result import EagerResult
from .result import Result
from .transaction import Transaction


class Session:
    def __init__(self, driver, session):
        self._driver = driver
        self._session = session

    def close(self, hooks=None):
        req = protocol.SessionClose(self._session.id)
        res = self._driver.send_and_receive(req, hooks=hooks,
                                            allow_resolution=False)
        if not isinstance(res, protocol.Session):
            raise Exception("Should be session but was: %s" % res)

    def run(self, cypher, params=None, tx_meta=None, hooks=None, **kwargs):
        req = protocol.SessionRun(self._session.id, cypher, params,
                                  txMeta=tx_meta, **kwargs)
        res = self._driver.send_and_receive(req, hooks=hooks,
                                            allow_resolution=True)
        if not isinstance(res, protocol.Result):
            raise Exception(
                "Should be Result or ResolverResolutionRequired "
                "but was: %s" % res
            )
        return Result(self._driver, res)

    def query(self, query, params=None, config=None, hooks=None):
        retry_func = None
        if config:
            retry_func = getattr(config, "retry_function", None)
        req = protocol.SessionQuery(self._session.id, query, params, config)
        res = self._driver.send_and_receive(
            req, hooks=hooks, allow_resolution=True
        )
        while isinstance(res, protocol.RetryFunc):
            handle_retry_func(retry_func, res, self._driver)
            res = self._driver.receive(hooks=hooks, allow_resolution=True)
        if not isinstance(res, protocol.EagerResult):
            raise Exception("Should be EagerResult or RetryFunc")
        return EagerResult(self, res)

    def execute(self, fn, config=None, hooks=None):
        retry_func = None
        if config:
            retry_func = getattr(config, "retry_function", None)
        req = protocol.SessionExecute(self._driver.id, config)
        return run_tx_loop(fn, req, self, retry_func=retry_func, hooks=hooks)

    def read_transaction(self, fn, tx_meta=None, hooks=None, **kwargs):
        # Send request to enter transactional read function
        req = protocol.SessionReadTransaction(
            self._session.id, txMeta=tx_meta, **kwargs
        )
        return run_tx_loop(fn, req, self._driver, hooks=hooks)

    def write_transaction(self, fn, tx_meta=None, hooks=None, **kwargs):
        # Send request to enter transactional read function
        req = protocol.SessionWriteTransaction(
            self._session.id, txMeta=tx_meta, **kwargs
        )
        return run_tx_loop(fn, req, self._driver, hooks=hooks)

    def begin_transaction(self, tx_meta=None, hooks=None, **kwargs):
        req = protocol.SessionBeginTransaction(
            self._session.id, txMeta=tx_meta, **kwargs
        )
        res = self._driver.send_and_receive(req, hooks=hooks,
                                            allow_resolution=True)
        if not isinstance(res, protocol.Transaction):
            raise Exception("Should be Transaction but was: %s" % res)
        return Transaction(self._driver, res.id)

    def last_bookmarks(self, hooks=None):
        req = protocol.SessionLastBookmarks(self._session.id)
        res = self._driver.send_and_receive(req, hooks=hooks,
                                            allow_resolution=False)
        if not isinstance(res, protocol.Bookmarks):
            raise Exception("Should be Bookmarks but was: %s" % res)
        return res.bookmarks
