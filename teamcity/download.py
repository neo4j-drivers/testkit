import json
from os import getenv
import re
from urllib import request

_ROOT = "https://live.neo4j-build.io"
_API_ROOT = f"{_ROOT}/app/rest"


class _AuthHandler(request.BaseHandler):
    def http_request(self, req):
        req.remove_header("Authorization")
        if (not req.has_header("Authorization")
                and req.full_url.startswith(_ROOT)):
            api_token = getenv("TEAMCITY_API_TOKEN")
            req.add_header("Authorization", f"Bearer {api_token}")

        return req

    https_request = http_request


class _JsonHandler(request.BaseHandler):
    def http_request(self, req):
        if not req.has_header("Accept"):
            req.add_header("Accept", "application/json")
        return req

    @staticmethod
    def _res_as_json(res):
        def read():
            if (
                "Content-Type" not in res.headers
                or res.headers["Content-Type"].lower() != "application/json"
            ):
                raise ValueError("Response is not json")
            encoding = res.info().get_content_charset("utf-8")
            data = res.read()
            return json.loads(data.decode(encoding))
        return read

    def http_response(self, req, res):
        res.read_json = self._res_as_json(res)
        return res

    https_request = http_request
    https_response = http_response


def _get_opener(json_=False):
    handlers = []
    if json_:
        handlers.append(_JsonHandler())
    handlers += [_AuthHandler(), request.HTTPRedirectHandler()]
    return request.build_opener(*handlers)


def _list_artifacts(filters=None, build_locator=None):
    if filters is None:
        filters = ()
    if not build_locator:
        raise ValueError("build_locator is required")
    path = \
        f"{_API_ROOT}/builds/{build_locator}/artifacts"
    artifacts = _get_opener(json_=True).open(path).read_json()
    return [
        artifact["content"]["href"]
        for artifact in artifacts["file"]
        if all(re.match(filter_, artifact["name"]) for filter_ in filters)
    ]


def _download_artifact(path):
    """
    Download a neo4j artifact from TeamCity.

    @returns: http response
    """
    path = _ROOT + path
    return _get_opener(json_=False).open(path)


class DockerImage:
    def __init__(self, *filters, build_type):
        self.filters = filters
        self.build_locator = f"buildType:{build_type}"

    def get(self):
        artifacts = _list_artifacts(filters=self.filters,
                                    build_locator=self.build_locator)
        if not len(artifacts) == 1:
            raise ValueError(
                "Filter matched not exactly one image.\n"
                f"Filters: {self.filters}\n"
                f"Matches: {artifacts}"
            )
        return _download_artifact(artifacts[0])
