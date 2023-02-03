import nutkit.protocol as types
from tests.stub.notification_config.notification_filters_base import (
    NotificationFiltersBase,
)


class TestDriverNotificationFilters(NotificationFiltersBase):
    required_features = (types.Feature.BOLT_5_1,
                         types.Feature.API_DRIVER_NOTIFICATION_CONFIG)

    def test_filter_notifications(self):
        for cfg in self.configs():
            with self.subTest(name=cfg["protocol"]):
                self._run_test_get_summary(cfg["protocol"], cfg["script"])
            self._server.reset()
