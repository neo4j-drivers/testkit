class NotificationFiltersBase:
    def configs(self):
        return [
            {
                "filters": ["None"],
                "params": {"#FILTERS#": "[]"}
            },
            {
                "filters": ["ALL.ALL"],
                "params": {"#FILTERS#": '["*.*"]'}
            },
            {
                "filters": ["ALL.QUERY"],
                "params": {"#FILTERS#": '["*.QUERY"]'}
            },
            {
                "filters": ["WARNING.ALL"],
                "params": {"#FILTERS#": '["WARNING.*"]'}
            },
            {
                "filters": ["WARNING.DEPRECATION"],
                "params": {"#FILTERS#": '["WARNING.DEPRECATION"]'}
            },
            {
                "filters": ["WARNING.HINT"],
                "params": {"#FILTERS#": '["WARNING.HINT"]'}
            },
            {
                "filters": ["WARNING.QUERY"],
                "params": {"#FILTERS#": '["WARNING.QUERY"]'}
            },
            {
                "filters": ["WARNING.UNSUPPORTED"],
                "params": {"#FILTERS#": '["WARNING.UNSUPPORTED"]'}
            },
            {
                "filters": ["INFORMATION.ALL"],
                "params": {"#FILTERS#": '["INFORMATION.*"]'}
            },
            {
                "filters": ["INFORMATION.RUNTIME"],
                "params": {"#FILTERS#": '["INFORMATION.RUNTIME"]'}
            },
            {
                "filters": ["INFORMATION.QUERY"],
                "params": {"#FILTERS#": '["INFORMATION.QUERY"]'}
            },
            {
                "filters": ["INFORMATION.PERFORMANCE"],
                "params": {"#FILTERS#": '["INFORMATION.PERFORMANCE"]'}
            },
            {
                "filters": ["INFORMATION.PERFORMANCE",
                            "WARNING.ALL", "ALL.QUERY"],
                "params": {"#FILTERS#": '["INFORMATION.PERFORMANCE", '
                                        '"WARNING.*", "*.QUERY"]'}
            }
        ]
