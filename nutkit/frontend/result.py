from .. import protocol


class Result:
    def __init__(self, driver, result):
        self._driver = driver
        self._result = result

    def next(self):
        """Move to next record in result."""
        req = protocol.ResultNext(self._result.id)
        return self._driver.send_and_receive(req, allow_resolution=True)

    def single(self):
        """Return one record if there is exactly one.

        Raises error otherwise.
        """
        req = protocol.ResultSingle(self._result.id)
        return self._driver.send_and_receive(req, allow_resolution=True)

    def single_optional(self):
        """Try leniently to fetch exactly one record.

        Return None, if there is no record.
        Return the record, if there is at least one.
        Warn, if there are more than one.
        """
        req = protocol.ResultSingleOptional(self._result.id)
        return self._driver.send_and_receive(req, allow_resolution=True)

    def scalar(self):
        """Unpack a single record with a single value.

        Raise error if there are not exactly one record or not exactly one
        value.
        """
        req = protocol.EagerResultScalar(self._result.id)
        return self._driver.send_and_receive(req, allow_resolution=True)

    def peek(self):
        """Return the next Record or NullRecord without consuming it."""
        req = protocol.ResultPeek(self._result.id)
        return self._driver.send_and_receive(req, allow_resolution=True)

    def consume(self):
        """Discard all records in result and returns summary."""
        req = protocol.ResultConsume(self._result.id)
        return self._driver.send_and_receive(req, allow_resolution=True)

    def list(self):
        """Retrieve the entire result stream."""
        req = protocol.ResultList(self._result.id)
        res = self._driver.send_and_receive(req, allow_resolution=True)
        assert isinstance(res, protocol.RecordList)
        return res.records

    def read_cypher_type_field(self, record_key, type_name, field_id):
        req = protocol.CypherTypeField(self._result.id, record_key, type_name,
                                       field_id)
        return self._driver.send_and_receive(req, allow_resolution=True)

    def keys(self):
        return self._result.keys

    def __iter__(self):
        while True:
            record = self.next()
            if isinstance(record, protocol.NullRecord):
                break
            yield record
