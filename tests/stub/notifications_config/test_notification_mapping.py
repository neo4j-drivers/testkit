import json

import nutkit.protocol as types
from tests.stub.notifications_config.notifications_base import (  # noqa: E501
    NotificationsBase,
)


class TestNotificationMapping(NotificationsBase):
    required_features = types.Feature.BOLT_5_2,

    def test_mapping(self):
        for config in self._map_test_configs():
            with self.subTest(name=config["notifications"]):
                self._run_test(config)
            self._server.reset()

    def _run_test(self, config):
        script = "notifications_mapping.script"
        emit = json.dumps(self._create_emit_param(config["notifications"]))
        script_params = {
            "#EMIT#": emit
        }
        driver_params = {
            "min_sev": None,
            "dis_cats": None
        }
        summary = self._run_test_get_summary(driver_params, script_params,
                                             script)
        notifications = self._reduce_notifications(summary.notifications)
        self.assertListEqual(config["expect"], notifications)

    @staticmethod
    def _reduce_notifications(notifications):
        return [
            {
                "category": x["category"],
                "category_string": x["rawCategory"],
                "severity": x["severityLevel"],
                "severity_string": x["rawSeverityLevel"]
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
                        "category": "HINT",
                        "severity": "INFORMATION"
                    }
                ],
                "expect": [
                    {
                        "category": "HINT",
                        "category_string": "HINT",
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
                        "category": "UNRECOGNIZED",
                        "severity": "INFORMATION"
                    }
                ],
                "expect": [
                    {
                        "category": "UNRECOGNIZED",
                        "category_string": "UNRECOGNIZED",
                        "severity": "INFORMATION",
                        "severity_string": "INFORMATION",
                    }
                ]
            },
            {
                "notifications": [
                    {
                        "category": "UNSUPPORTED",
                        "severity": "INFORMATION"
                    }
                ],
                "expect": [
                    {
                        "category": "UNSUPPORTED",
                        "category_string": "UNSUPPORTED",
                        "severity": "INFORMATION",
                        "severity_string": "INFORMATION",
                    }
                ]
            },
            {
                "notifications": [
                    {
                        "category": "GENERIC",
                        "severity": "INFORMATION"
                    }
                ],
                "expect": [
                    {
                        "category": "GENERIC",
                        "category_string": "GENERIC",
                        "severity": "INFORMATION",
                        "severity_string": "INFORMATION",
                    }
                ]
            },
            {
                "notifications": [
                    {
                        "category": "PERFORMANCE",
                        "severity": "INFORMATION"
                    }
                ],
                "expect": [
                    {
                        "category": "PERFORMANCE",
                        "category_string": "PERFORMANCE",
                        "severity": "INFORMATION",
                        "severity_string": "INFORMATION",
                    }
                ]
            },
            {
                "notifications": [
                    {
                        "category": "SECURITY",
                        "severity": "INFORMATION"
                    }
                ],
                "expect": [
                    {
                        "category": "SECURITY",
                        "category_string": "SECURITY",
                        "severity": "INFORMATION",
                        "severity_string": "INFORMATION",
                    }
                ]
            },
            {
                "notifications": [
                    {
                        "category": "TOPOLOGY",
                        "severity": "INFORMATION"
                    }
                ],
                "expect": [
                    {
                        "category": "TOPOLOGY",
                        "category_string": "TOPOLOGY",
                        "severity": "INFORMATION",
                        "severity_string": "INFORMATION",
                    }
                ]
            },
            {
                "notifications": [
                    {
                        "category": "SCHEMA",
                        "severity": "INFORMATION"
                    }
                ],
                "expect": [
                    {
                        "category": "SCHEMA",
                        "category_string": "SCHEMA",
                        "severity": "INFORMATION",
                        "severity_string": "INFORMATION",
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
                        "category": "UNRECOGNIZED",
                        "severity": "ERROR"
                    }
                ],
                "expect": [
                    {
                        "category": "UNRECOGNIZED",
                        "category_string": "UNRECOGNIZED",
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
