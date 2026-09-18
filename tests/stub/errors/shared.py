__all__ = [
    "DEFAULT_DIAG_REC",
    "default_gql_error_description",
    "DEFAULT_GQL_ERROR_STATUS",
]

DEFAULT_DIAG_REC = {
    "CURRENT_SCHEMA": "/",
    "OPERATION": "",
    "OPERATION_CODE": "0",
}
DEFAULT_GQL_ERROR_STATUS = "50N42"


def default_gql_error_description(error_message: str) -> str:
    return (
        "error: general processing exception - unexpected error. "
        f"{error_message}"
    )
