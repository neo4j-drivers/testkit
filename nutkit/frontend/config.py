from dataclasses import dataclass
from typing import (
    Callable,
    Optional,
)


@dataclass
class DefaultBookmarkManagerConfig:
    initial_bookmarks: Optional[dict[str, list[str]]] = None
    bookmark_supplier: Optional[Callable[[str], list[str]]] = None
    notify_bookmarks: Optional[Callable[[str, str], None]] = None


def to_protocol(
    config: Optional[DefaultBookmarkManagerConfig]
) -> Optional[dict]:
    if config is not None:
        return {
            "initialBookmarks": config.initial_bookmarks,
            "bookmarkSupplier": config.bookmark_supplier is not None,
            "notifyBookmarks": config.notify_bookmarks is not None,
        }

    return None
