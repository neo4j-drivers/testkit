from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import (
    Any,
    Callable,
    Collection,
    Dict,
    List,
    Optional,
    Tuple,
)

# exception, attempt, maxAttempts
_T_RetryFunc = Callable[[Exception, int, int], Tuple[bool, int]]


class ClusterAccessMode(Enum):
    NAIVE = "naive"
    READERS = "readers"
    WRITERS = "writers"


class TxClusterMemberAccess(Enum):
    READERS = "readers"
    WRITERS = "writers"


class _BaseConf:
    _attr_to_conf_key = {}

    def to_protocol(self) -> Dict[str, Any]:
        return {
            self._attr_to_conf_key[k]: v
            for k, v in vars(self).items() if v is not None
        }


@dataclass
class QueryConfig(_BaseConf):
    max_record_count: Optional[int] = None
    skip_records: Optional[bool] = None

    _attr_to_conf_key = {
        **_BaseConf._attr_to_conf_key,
        "max_record_count": "maxRecordCount",
        "skip_records": "skipRecords",
    }


@dataclass
class SessionQueryConfig(QueryConfig):
    cluster_access_mode: Optional[ClusterAccessMode] = None
    timeout: Optional[int] = None
    metadata: Optional[Dict[str, Any]] = None
    max_retries: Optional[int] = None
    # TODO: what is RetryInfo?
    retry_function: Optional[_T_RetryFunc] = None
    execute_in_transaction: Optional[bool] = None

    _attr_to_conf_key = {
        **QueryConfig._attr_to_conf_key,
        "cluster_access_mode": "clusterAccessMode",
        "timeout": "timeout",
        "metadata": "metadata",
        "max_retries": "maxRetries",
        "retry_function": "retryFunctionRegistered",
        "execute_in_transaction": "executeInTransaction",
    }

    def to_protocol(self) -> Dict[str, Any]:
        res = super().to_protocol()
        res["retryFunctionRegistered"] = self.retry_function is not None
        return res


@dataclass
class DriverQueryConfig(SessionQueryConfig):
    database: Optional[str] = None
    bookmarks: Optional[Collection[str]] = None
    impersonated_user: Optional[str] = None

    _attr_to_conf_key = {
        **SessionQueryConfig._attr_to_conf_key,
        "database": "database",
        "bookmarks": "bookmarks",
        "impersonated_user": "impersonatedUser",
    }


@dataclass
class SessionTxConfig(_BaseConf):
    timeout: Optional[int] = None
    metadata: Optional[Dict[str, Any]] = None
    max_retries: Optional[int] = None
    # TODO: what is RetryInfo?
    retry_function: Optional[_T_RetryFunc] = None

    _attr_to_conf_key = {
        **_BaseConf._attr_to_conf_key,
        "timeout": "timeout",
        "metadata": "metadata",
        "max_retries": "maxRetries",
        "retry_function": "retryFunctionRegistered",
    }

    def to_protocol(self) -> Dict[str, Any]:
        res = super().to_protocol()
        res["retryFunctionRegistered"] = self.retry_function is not None
        return res


@dataclass
class DriverTxConfig(SessionTxConfig):
    database: Optional[str] = None
    bookmarks: Optional[Collection[str]] = None
    impersonated_user: Optional[str] = None

    _attr_to_conf_key = {
        **SessionTxConfig._attr_to_conf_key,
        "database": "database",
        "bookmarks": "bookmarks",
        "impersonated_user": "impersonatedUser",
    }


@dataclass
class Neo4jBookmarkManagerConfig:
    initial_bookmarks: Optional[Dict[str, List[str]]] = None
    bookmark_supplier: Optional[Callable[[str], List[str]]] = None
    notify_bookmarks: Optional[Callable[[str, str], None]] = None


def from_bookmark_manager_config_to_protocol(
    config: Optional[Neo4jBookmarkManagerConfig]
) -> Optional[Dict]:
    if config is not None:
        return {
            "initialBookmarks": config.initial_bookmarks,
            "bookmarkSupplierRegistered": config.bookmark_supplier is not None,
            "notifyBookmarksRegistered": config.notify_bookmarks is not None,
        }

    return None
