from .. import protocol


class EagerResult:
    def __init__(self, driver, eager_result):
        self._driver = driver
        self._eager_result = eager_result
        self.keys = eager_result.keys
        self.records = eager_result.records
        self.summary = eager_result.summary

    def single(self):
        """Return one record if there is exactly one.

        Raises error otherwise.
        """
        req = protocol.EagerResultSingle(self._eager_result.id)
        return self._driver.send_and_receive(req, allow_resolution=True)

    def scalar(self):
        """Unpack a single record with a single value.

        Raise error if there are not exactly one record or not exactly one
        value.
        """
        req = protocol.EagerResultScalar(self._eager_result.id)
        return self._driver.send_and_receive(req, allow_resolution=True)
