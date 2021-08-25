from .rx_transaction_completion_publisher import \
    RxTransactionCompletionPublisher
from .. import protocol


class ApplicationCodeException(Exception):
    pass


class RxSession:
    def __init__(self, driver, backend, session):
        self._driver = driver
        self._backend = backend
        self._session = session

    def write_transaction(self, work_fn):
        req = protocol.RxSessionWriteTransaction(self._session.id)
        res = self._backend.sendAndReceive(req)
        if not isinstance(res, protocol.Session):
            raise Exception("Should be Session")
        return RxTransactionCompletionPublisher(driver=self._driver,
                                                backend=self._backend,
                                                session=self._session,
                                                work_fn=work_fn)
