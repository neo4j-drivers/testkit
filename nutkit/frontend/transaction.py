from .. import protocol
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
