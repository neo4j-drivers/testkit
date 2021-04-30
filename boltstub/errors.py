class ServerExit(Exception):
    pass


class BoltProtocolError(Exception):
    pass


class BoltUnknownVersion(BoltProtocolError):
    pass


class BoltMissingVersion(BoltProtocolError):
    pass


class BoltUnknownMessage(BoltProtocolError):
    def __init__(self, msg, line):
        super().__init__(msg, line)

    @property
    def msg(self):
        return self.args[0]

    @property
    def line(self):
        return self.args[1]
