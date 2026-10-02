from __future__ import annotations

import secrets
from typing import (
    Any,
    ClassVar,
)

from ..protocol import (
    EncapsulatedKeyRepositoryCreateCompleted,
    EncapsulatedKeyRepositoryCreateRequest,
    EncapsulatedKeyRepositoryDeleteCompleted,
    EncapsulatedKeyRepositoryDeleteRequest,
    EncapsulatedKeyRepositoryErrorCompleted,
    EncapsulatedKeyRepositoryFindByAliasCompleted,
    EncapsulatedKeyRepositoryFindByAliasRequest,
    EncapsulatedKeyRepositoryFindByIdCompleted,
    EncapsulatedKeyRepositoryFindByIdRequest,
    EncapsulatedKeyRepositoryImportCompleted,
    EncapsulatedKeyRepositoryImportRequest,
    EncapsulatedKeyRepositorySetAliasCompleted,
    EncapsulatedKeyRepositorySetAliasRequest,
)

__all__ = ["EncapsulatedKeyRepository"]


class _RepositoryError(Exception):
    def __init__(self, error_type: str, detail: str):
        super().__init__(error_type, detail)
        self.error_type = error_type
        self.detail = detail


class _Store:
    """One backend-configured repository's key and alias data."""

    def __init__(self):
        self._keys_by_id: dict[str, dict] = {}
        self._id_by_alias: dict[str, str] = {}

    def find_by_id(self, key_id: str) -> dict | None:
        return self._keys_by_id.get(key_id)

    def find_by_alias(self, alias: str) -> dict | None:
        key_id = self._id_by_alias.get(alias)
        return self._keys_by_id.get(key_id) if key_id is not None else None

    def store(
        self, key_id: str, alias: str | None, encapsulation: Any,
        metadata: Any
    ) -> dict:
        if alias is not None:
            self._ensure_alias_free(alias, key_id)
            self._id_by_alias[alias] = key_id

        record = {
            "id": key_id,
            "alias": alias,
            "encapsulation": encapsulation,
            "metadata": metadata,
        }
        self._keys_by_id[key_id] = record
        return record

    def set_alias(self, key_id: str, alias: str | None) -> None:
        record = self._get_or_raise(key_id)
        if alias is not None:
            self._ensure_alias_free(alias, key_id)

        old_alias = record["alias"]
        if old_alias is not None:
            self._id_by_alias.pop(old_alias, None)
        if alias is not None:
            self._id_by_alias[alias] = key_id

        record["alias"] = alias

    def delete(self, key_id: str) -> None:
        record = self._get_or_raise(key_id)
        if record["alias"] is not None:
            self._id_by_alias.pop(record["alias"], None)
        del self._keys_by_id[key_id]

    def _ensure_alias_free(self, alias: str, id_claiming_it: str) -> None:
        owner = self._id_by_alias.get(alias)
        if owner is not None and owner != id_claiming_it:
            raise _RepositoryError("AliasInUse", alias)

    def _get_or_raise(self, key_id: str) -> dict:
        record = self._keys_by_id.get(key_id)
        if record is None:
            raise _RepositoryError("KeyNotFound", key_id)
        return record


class EncapsulatedKeyRepository:
    """
    Default, dict-backed encapsulated key repository.

    Lives on the TestKit frontend, reachable by the backend through reverse
    requests. Storage for one backend-configured repository is created
    lazily, keyed by whatever repository id the backend assigns when the
    profile is configured — there is no separate registration round trip.
    """

    _stores: ClassVar[dict[str, _Store]] = {}

    @classmethod
    def process_callbacks(cls, request):
        if isinstance(request, EncapsulatedKeyRepositoryFindByIdRequest):
            return cls._find_by_id(request)
        if isinstance(request, EncapsulatedKeyRepositoryFindByAliasRequest):
            return cls._find_by_alias(request)
        if isinstance(request, EncapsulatedKeyRepositoryCreateRequest):
            return cls._create(request)
        if isinstance(request, EncapsulatedKeyRepositoryImportRequest):
            return cls._import(request)
        if isinstance(request, EncapsulatedKeyRepositorySetAliasRequest):
            return cls._set_alias(request)
        if isinstance(request, EncapsulatedKeyRepositoryDeleteRequest):
            return cls._delete(request)
        return None

    @classmethod
    def _store_for(cls, repository_id: str) -> _Store:
        return cls._stores.setdefault(repository_id, _Store())

    @classmethod
    def _find_by_id(cls, request):
        record = cls._store_for(request.repository_id).find_by_id(
            request.key_id
        )
        return EncapsulatedKeyRepositoryFindByIdCompleted(request.id, record)

    @classmethod
    def _find_by_alias(cls, request):
        record = cls._store_for(request.repository_id).find_by_alias(
            request.alias
        )
        return EncapsulatedKeyRepositoryFindByAliasCompleted(
            request.id, record
        )

    @classmethod
    def _create(cls, request):
        try:
            key_id = secrets.token_hex(8)
            record = cls._store_for(request.repository_id).store(
                key_id, request.alias, request.encapsulation,
                request.metadata
            )
        except _RepositoryError as error:
            return cls._error_completed(request.id, error)

        return EncapsulatedKeyRepositoryCreateCompleted(request.id, record)

    @classmethod
    def _import(cls, request):
        try:
            record = cls._store_for(request.repository_id).store(
                request.key_id, request.alias, request.encapsulation,
                request.metadata
            )
        except _RepositoryError as error:
            return cls._error_completed(request.id, error)

        return EncapsulatedKeyRepositoryImportCompleted(request.id, record)

    @classmethod
    def _set_alias(cls, request):
        try:
            cls._store_for(request.repository_id).set_alias(
                request.key_id, request.alias
            )
        except _RepositoryError as error:
            return cls._error_completed(request.id, error)

        return EncapsulatedKeyRepositorySetAliasCompleted(request.id)

    @classmethod
    def _delete(cls, request):
        try:
            cls._store_for(request.repository_id).delete(request.key_id)
        except _RepositoryError as error:
            return cls._error_completed(request.id, error)

        return EncapsulatedKeyRepositoryDeleteCompleted(request.id)

    @staticmethod
    def _error_completed(request_id, error: _RepositoryError):
        return EncapsulatedKeyRepositoryErrorCompleted(
            request_id, error.error_type, error.detail
        )
