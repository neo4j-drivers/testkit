from boltstub import BoltStubService


SCRIPT = """\
!: BOLT 4.1
!: PORT 17687
!: AUTO RESET
!: AUTO HELLO
!: AUTO GOODBYE

C: RUN "RETURN 1 as n" {} {}
   PULL {"n": 1000}
S: SUCCESS {"fields": ["n"]}
   RECORD [1]
   SUCCESS {}
"""


def main():
    service = BoltStubService.from_strings(SCRIPT)
    try:
        service.start()

    finally:
        service.stop()


if __name__ == "__main__":
    main()
