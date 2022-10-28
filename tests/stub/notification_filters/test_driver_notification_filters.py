import nutkit.protocol as types
from tests.stub.notification_filters.notification_filters_base import (
    NotificationFiltersBase,
)


class TestDriverNotificationFilters(NotificationFiltersBase):
    required_features = (types.Feature.BOLT_5_1,
                         types.Feature.API_DRIVER_NOTIFICATION_FILTERS)

    def test_default_server_filters(self):
        no_filter_script = "driver_default_notification_filters.script"
        self._run_test_get_summary(None, None, no_filter_script)

    def test_filter_notifications(self):
        for cfg in self.configs():
            with self.subTest(name=cfg["filters"]):
                self._run_test_get_summary(cfg["filters"], cfg["params"])
