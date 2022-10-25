import nutkit.protocol as types
from tests.stub.notification_filters.notification_filters_base import (
    NotificationFiltersBase,
)


class TestNotificationMapping(NotificationFiltersBase):
    required_features = types.Feature.BOLT_5_1,

    # def test_mapping(self):
    #     for config in self._map_test_configs():
    #         with self.subTest(name=config["name"]):
    #             self._run_test(config)

    def _run_test(self, config):
        super()._run_test_get_summary(config)

    @staticmethod
    def _map_test_configs():
        return [
            {

            }
        ]
