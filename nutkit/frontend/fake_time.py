from .. import protocol
from ..backend import Backend


class FakeTime:

    def __init__(self, backend: Backend):
        self._backend = backend

    def __enter__(self):
        req = protocol.FakeTimeInstall()
        res = self._backend.send_and_receive(req)
        if not isinstance(res, protocol.FakeTimeAck):
            raise Exception(f"Should be FakeTimeAck but was {res}")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        req = protocol.FakeTimeUninstall()
        res = self._backend.send_and_receive(req)
        if not isinstance(res, protocol.FakeTimeAck):
            raise Exception(f"Should be FakeTimeAck but was {res}")

    def tick(self, increment_ms: int):
        req = protocol.FakeTimeTick(increment_ms)
        res = self._backend.send_and_receive(req)
        if not isinstance(res, protocol.FakeTimeAck):
            raise Exception(f"Should be FakeTimeAck but was {res}")
