import  os, shutil

if __name__ == "__main__":
    # Cleanup artifacts
    artifactsPath = "/artifacts"
    for name in os.listdir(artifactsPath):
        path = os.path.join(artifactsPath, name)
        if os.path.isfile(path):
            os.unlink(path)
        else:
            shutil.rmtree(path)

