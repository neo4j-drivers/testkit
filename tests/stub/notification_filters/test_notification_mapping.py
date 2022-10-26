import json

import nutkit.protocol as types
from tests.stub.notification_filters.notification_filters_base import (
    NotificationFiltersBase,
)


class TestNotificationMapping(NotificationFiltersBase):
    required_features = types.Feature.BOLT_5_1,

    def test_mapping(self):
        for config in self._map_test_configs():
            with self.subTest(name=config["notifications"]):
                self._run_test(config)

    def _run_test(self, config):
        script = "emit_notifications.script"
        emit = json.dumps(self._create_emit_param(config["notifications"]))
        params = {
            "#EMIT#": emit
        }
        summary = super()._run_test_get_summary(["ALL.ALL"], params, script)
        notifications = self._reduce_notifications(summary.notifications)
        self.assertListEqual(config["expect"], notifications)

    @staticmethod
    def _reduce_notifications(notifications):
        return [
            {
                "category": x["category"],
                "category_string": x["rawCategory"],
                "severity": x["severityLevel"],
                "severity_string": x["severity"]
            } for x in notifications
        ]

    @staticmethod
    def _create_emit_param(notifications):
        return [
            {
                "category": x["category"],
                "severity": x["severity"],
                "code": "ignore",
                "title": "ignore",
                "description": "ignore"
            }
            for x in notifications
        ]

    @staticmethod
    def _map_test_configs():
        return [
            {
                "notifications": [
                    {
                        "category": "RUNTIME",
                        "severity": "INFORMATION"
                    }
                ],
                "expect": [
                    {
                        "category": "RUNTIME",
                        "category_string": "RUNTIME",
                        "severity": "INFORMATION",
                        "severity_string": "INFORMATION",
                    }
                ]
            },
            {
                "notifications": [
                    {
                        "category": "DEPRECATION",
                        "severity": "WARNING"
                    }
                ],
                "expect": [
                    {
                        "category": "DEPRECATION",
                        "category_string": "DEPRECATION",
                        "severity": "WARNING",
                        "severity_string": "WARNING",
                    }
                ]
            },
            {
                "notifications": [
                    {
                        "category": "MADE_UP",
                        "severity": "WARNING"
                    }
                ],
                "expect": [
                    {
                        "category": "UNKNOWN",
                        "category_string": "MADE_UP",
                        "severity": "WARNING",
                        "severity_string": "WARNING",
                    }
                ]
            },
            {
                "notifications": [
                    {
                        "category": "MADE_UP",
                        "severity": "INFORMATION"
                    }
                ],
                "expect": [
                    {
                        "category": "UNKNOWN",
                        "category_string": "MADE_UP",
                        "severity": "INFORMATION",
                        "severity_string": "INFORMATION",
                    }
                ]
            },
            {
                "notifications": [
                    {
                        "category": "RUNTIME",
                        "severity": "ERROR"
                    }
                ],
                "expect": [
                    {
                        "category": "RUNTIME",
                        "category_string": "RUNTIME",
                        "severity": "UNKNOWN",
                        "severity_string": "ERROR",
                    }
                ]
            },
            {
                "notifications": [
                    {
                        "category": "MADE_UP",
                        "severity": "ERROR"
                    }
                ],
                "expect": [
                    {
                        "category": "UNKNOWN",
                        "category_string": "MADE_UP",
                        "severity": "UNKNOWN",
                        "severity_string": "ERROR",
                    }
                ]
            },
            {
                "notifications": [
                    {
                        "category": "MADE_UP",
                        "severity": "ERROR"
                    },
                    {
                        "category": "DEPRECATION",
                        "severity": "WARNING"
                    }
                ],
                "expect": [
                    {
                        "category": "UNKNOWN",
                        "category_string": "MADE_UP",
                        "severity": "UNKNOWN",
                        "severity_string": "ERROR",
                    },
                    {
                        "category": "DEPRECATION",
                        "category_string": "DEPRECATION",
                        "severity": "WARNING",
                        "severity_string": "WARNING",
                    }
                ]
            }
        ]
