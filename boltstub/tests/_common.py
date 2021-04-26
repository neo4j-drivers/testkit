ALL_SERVER_VERSIONS = (1,), (2,), (3,), (4, 1), (4, 2), (4, 3)


ALL_REQUESTS_PER_VERSION = tuple((
    *(((major,), tag, name)
      for major in (1, 2)
      for (tag, name) in (
          (b"\x01", "INIT"),
          (b"\x0E", "ACK_FAILURE"),
          (b"\x0F", "RESET"),
          (b"\x10", "RUN"),
          (b"\x2F", "DISCARD_ALL"),
          (b"\x3F", "PULL_ALL"),
      )),

    ((3,), b"\x01", "HELLO"),
    ((3,), b"\x02", "GOODBYE"),
    ((3,), b"\x0F", "RESET"),
    ((3,), b"\x10", "RUN"),
    ((3,), b"\x2F", "DISCARD_ALL"),
    ((3,), b"\x3F", "PULL_ALL"),
    ((3,), b"\x11", "BEGIN"),
    ((3,), b"\x12", "COMMIT"),
    ((3,), b"\x13", "ROLLBACK"),

    *(((4, minor), tag, name)
      for minor in range(4)
      for (tag, name) in (
          (b"\x01", "HELLO"),
          (b"\x02", "GOODBYE"),
          (b"\x0F", "RESET"),
          (b"\x10", "RUN"),
          (b"\x2F", "DISCARD"),
          (b"\x3F", "PULL"),
          (b"\x11", "BEGIN"),
          (b"\x12", "COMMIT"),
          (b"\x13", "ROLLBACK"),
      ))
))


ALL_RESPONSES_PER_VERSION = tuple((
    *((version, tag, name)
      for version in ALL_SERVER_VERSIONS
      for (tag, name) in (
          (b"\x70", "SUCCESS"),
          (b"\x7E", "IGNORED"),
          (b"\x7F", "FAILURE"),
          (b"\x71", "RECORD"),
      )),
))
