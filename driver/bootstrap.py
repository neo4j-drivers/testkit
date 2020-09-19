import sys, time

if __name__ == "__main__":
    # This notifies controller that we're up and running in the driver
    print("ok")
    sys.stdout.flush()
    # Just hang around here
    while True:
        time.sleep(100)

