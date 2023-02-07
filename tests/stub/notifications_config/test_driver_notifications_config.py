import nutkit.protocol as types
from tests.stub.notifications_config.notifications_base import (  # noqa: E501
    NotificationsBase,
)


class TestDriverNotificationsConfig(NotificationsBase):
    required_features = (types.Feature.BOLT_5_2,
                         types.Feature.API_DRIVER_NOTIFICATIONS_CONFIG)

    def test_driver_notifications_config_on_hello(self):
        for cfg in self.configs():
            with self.subTest(name=cfg["protocol"]):
                self._run_test_get_summary(cfg["protocol"], cfg["script"])
            self._server.reset()
