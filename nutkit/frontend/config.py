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
    bookmark_supplier: Optional[Callable[[str], List[str]]] = None
    notify_bookmarks: Optional[Callable[[str, str], None]] = None


def to_protocol(
    config: Optional[Neo4jBookmarkManagerConfig]
) -> Optional[Dict]:
    if config is not None:
        return {
            "initialBookmarks": config.initial_bookmarks,
            "bookmarkSupplier": config.bookmark_supplier is not None,
            "notifyBookmarks": config.notify_bookmarks is not None,
        }

    return None
