from nutkit import protocol as types
from nutkit.frontend import Driver
from tests.shared import (
    get_driver_name,
    TestkitTestCase,
)
from tests.stub.shared import StubServer


class TestDriverQuery(TestkitTestCase):

    required_features = types.Feature.BOLT_5_0,

    ...
