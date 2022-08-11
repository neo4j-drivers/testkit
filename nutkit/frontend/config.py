from dataclasses import dataclass
from typing import (
    Callable,
    Dict,
    List,
    Optional,
)


@dataclass
class Neo4jBookmarkManagerConfig:
    initial_bookmarks: Optional[Dict[str, List[str]]] = None
    bookmarks_supplier: Optional[Callable[[str], List[str]]] = None
    bookmarks_consumer: Optional[Callable[[str, List[str]], None]] = None


def from_bookmark_manager_config_to_protocol(
    config: Optional[Neo4jBookmarkManagerConfig]
) -> Optional[Dict]:
    if config is not None:
        return {
            "initialBookmarks": config.initial_bookmarks,
            "bookmarksSupplierRegistered":
                config.bookmarks_supplier is not None,
            "bookmarksConsumerRegistred":
                config.bookmarks_consumer is not None,
        }

    return None
