def hex_repr(b, upper=True):
    if upper:
        return " ".join("{:02X}".format(x) for x in b)
    else:
        return " ".join("{:02x}".format(x) for x in b)


def recursive_subclasses(cls):
    for s_cls in cls.__subclasses__():
        yield s_cls
        for s_s_cls in recursive_subclasses(s_cls):
            yield s_s_cls
