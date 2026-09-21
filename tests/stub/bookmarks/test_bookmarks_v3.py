from nutkit import protocol as types
from tests.stub.bookmarks import test_bookmarks_v4


class TestBookmarksV3(test_bookmarks_v4.TestBookmarksV4):

    required_features = types.Feature.BOLT_3_0,

    version_dir = "v3"
