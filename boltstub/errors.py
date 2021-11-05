class ServerExit(Exception):  # noqa: N818
    pass


class BoltProtocolError(Exception):
    pass


class BoltUnknownVersionError(BoltProtocolError):
    pass


class BoltMissingVersionError(BoltProtocolError):
    pass


class BoltUnknownMessageError(BoltProtocolError):
    def __init__(self, msg, line):
        super().__init__(msg, line)

    @property
    def msg(self):
        return self.args[0]

    @property
    def line(self):
        return self.args[1]
