from dataclasses import dataclass
from typing import (
    Callable,
    Dict,
    List,
    Optional,
)

from nutkit.backend import Backend

from .. import protocol


@dataclass
class Neo4jBookmarkManagerConfig:
    initial_bookmarks: Optional[Dict[str, List[str]]] = None
    bookmarks_supplier: Optional[Callable[[str], List[str]]] = None
    bookmarks_consumer: Optional[Callable[[str, List[str]], None]] = None


@dataclass
class BookmarkManager:
    config: Neo4jBookmarkManagerConfig
    id: int


def create_bookmark_manager(backend: Backend,
                            config: Neo4jBookmarkManagerConfig):
    req = protocol.NewBookmarkManager(
        config.initial_bookmarks,
        config.bookmarks_supplier is not None,
        config.bookmarks_consumer is not None
    )
    res = backend.send_and_receive(req)
    if not isinstance(res, protocol.BookmarkManager):
        raise Exception("Should be BookmarkManager but was %s" % res)

    return BookmarkManager(
        config,
        res.id
    )
