from urllib import request
from os import getenv
import subprocess


def download_artifact(build_id, build_spec, path):
    """ Returns http response
    """
    root = "https://live.neo4j-build.io"
    path = "{}/repository/download/{}/{}/{}".format(root, build_id, build_spec, path)
    user = getenv("TEAMCITY_USER")
    pasw = getenv("TEAMCITY_PASSWORD")
    password_mgr = request.HTTPPasswordMgrWithDefaultRealm()
    password_mgr.add_password(None, root, user, pasw)
    handler = request.HTTPBasicAuthHandler(password_mgr)
    opener = request.build_opener(handler)
    return opener.open(path)


class DockerImage:
    def __init__(self, name):
        self.name = name

    def get(self):
        return download_artifact("DriversTestkitNeo4jDockers", ".lastSuccessful", "neo4j-docker/" + self.name)

