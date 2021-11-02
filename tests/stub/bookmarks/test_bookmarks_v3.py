from nutkit import protocol as types

from tests.stub.bookmarks.test_bookmarks_v4 import TestBookmarksV4


class TestBookmarksV3(TestBookmarksV4):

    required_features = types.Feature.BOLT_3_0,

    version_dir = "v3"
