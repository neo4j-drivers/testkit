from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import (
    Any,
    ClassVar,
)

from nutkit.backend import Backend

from .. import protocol


@dataclass
class Neo4jBookmarkManagerConfig:
    initial_bookmarks: list[str] | None = None
    bookmarks_supplier: Callable[[], list[str]] | None = None
    bookmarks_consumer: Callable[[list[str]], None] | None = None


class BookmarkManager:
    _registry: ClassVar[dict[Any, BookmarkManager]] = {}

    def __init__(self, backend: Backend, config: Neo4jBookmarkManagerConfig):
        self._backend = backend
        self.config = config

        req = protocol.NewBookmarkManager(
            config.initial_bookmarks,
            config.bookmarks_supplier is not None,
            config.bookmarks_consumer is not None
        )
        res = backend.send_and_receive(req)
        if not isinstance(res, protocol.BookmarkManager):
            raise Exception("Should be BookmarkManager but was %s" % res)

        self._bookmark_manager = res
        self._registry[self._bookmark_manager.id] = self

    @property
    def id(self):
        return self._bookmark_manager.id

    @classmethod
    def process_callbacks(cls, request):
        if isinstance(request, protocol.BookmarksSupplierRequest):
            if request.bookmark_manager_id not in cls._registry:
                raise Exception(
                    "Backend provided unknown Bookmark manager id: "
                    f"{request.bookmark_manager_id} not found"
                )

            manager = cls._registry[request.bookmark_manager_id]
            supply = manager.config.bookmarks_supplier
            if supply is not None:
                bookmarks = supply()
                return protocol.BookmarksSupplierCompleted(request.id,
                                                           bookmarks)
        if isinstance(request, protocol.BookmarksConsumerRequest):
            if request.bookmark_manager_id not in cls._registry:
                raise ValueError(
                    "Backend provided unknown Bookmark manager id: "
                    f"{request.bookmark_manager_id} not found"
                )
            manager = cls._registry[request.bookmark_manager_id]
            consume = manager.config.bookmarks_consumer
            if consume is not None:
                consume(request.bookmarks)
                return protocol.BookmarksConsumerCompleted(request.id)

    def close(self, hooks=None):
        res = self._backend.send_and_receive(
            protocol.BookmarkManagerClose(self.id),
            hooks=hooks
        )
        if not isinstance(res, protocol.BookmarkManager):
            raise Exception("Should be BookmarkManager but was %s" % res)
        del self._registry[self.id]
