import sys, time, os, shutil

if __name__ == "__main__":
    # Cleanup artifacts
    artifactsPath = "/artifacts"
    for name in os.listdir(artifactsPath):
        path = os.path.join(artifactsPath, name)
        if os.path.isfile(path):
            os.unlink(path)
        else:
            shutil.rmtree(path)

    # This notifies controller that we're up and running in the driver
    print("ok")
    sys.stdout.flush()
    # Just hang around here
    while True:
        time.sleep(1)

