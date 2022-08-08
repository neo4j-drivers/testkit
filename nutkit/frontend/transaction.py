from .. import protocol
from ._transaction_loop import handle_retry_func
from .eager_result import EagerResult
from .result import Result


class Transaction:
    def __init__(self, driver, id_):
        self._driver = driver
        self._id = id_

    def run(self, cypher, params=None):
        req = protocol.TransactionRun(self._id, cypher, params)
        res = self._driver.send_and_receive(req, allow_resolution=True)
        if not isinstance(res, protocol.Result):
            raise Exception("Should be result but was: %s" % res)
        return Result(self._driver, res)

    def query(self, query, params=None, config=None, hooks=None):
        retry_func = None
        if config:
            retry_func = getattr(config, "retry_function", None)
        req = protocol.TransactionQuery(self._id, query, params, config)
        res = self._driver.send_and_receive(
            req, hooks=hooks, allow_resolution=True
        )
        while isinstance(res, protocol.RetryFunc):
            handle_retry_func(retry_func, res, self._driver)
            res = self._driver.receive(hooks=hooks, allow_resolution=True)
        if not isinstance(res, protocol.EagerResult):
            raise Exception("Should be EagerResult or RetryFunc")
        return EagerResult(self, res)

    def commit(self):
        req = protocol.TransactionCommit(self._id)
        res = self._driver.send_and_receive(req, allow_resolution=True)
        if not isinstance(res, protocol.Transaction):
            raise Exception("Should be transaction but was: %s" % res)

    def rollback(self):
        req = protocol.TransactionRollback(self._id)
        res = self._driver.send_and_receive(req, allow_resolution=True)
        if not isinstance(res, protocol.Transaction):
            raise Exception("Should be transaction but was: %s" % res)

    def close(self):
        req = protocol.TransactionClose(self._id)
        res = self._driver.send_and_receive(req, allow_resolution=True)
        if not isinstance(res, protocol.Transaction):
            raise Exception("Should be transaction but was: %s" % res)
