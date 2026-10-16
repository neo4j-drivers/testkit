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
    EncapsulatedKeyRepositorySetAliasCompleted,
    EncapsulatedKeyRepositorySetAliasRequest,
)

__all__ = ["EncapsulatedKeyRepository", "InvalidRepositoryRequestError"]


class InvalidRepositoryRequestError(Exception):
    """
    The backend sent a repository request that breaks the protocol.

    Carries the reply to send so the backend is not left waiting.
    """

    def __init__(self, request_id, error_type, detail, message):
        super().__init__(request_id, error_type, detail, message)
        self.message = message
        self.reply = EncapsulatedKeyRepositoryErrorCompleted(
            request_id, error_type, detail
        )

    def __str__(self):
        return self.message

    @classmethod
    def unknown_repository(cls, request_id, repository_id):
        return cls(
            request_id, "UnknownRepository", repository_id,
            f"Backend used repository '{repository_id}', which TestKit did "
            "not assign to a live driver"
        )

    @classmethod
    def invalid_encapsulation(cls, request_id, encapsulation):
        return cls(
            request_id, "InvalidEncapsulation", encapsulation,
            f"Backend sent encapsulation '{encapsulation}', which is not a "
            "hex string"
        )


class _RepositoryError(Exception):
    def __init__(self, error_type: str, detail: str):
        super().__init__(error_type, detail)
        self.error_type = error_type
        self.detail = detail


class _Store:
    """One repository's key and alias data."""

    def __init__(self):
        self._keys_by_id: dict[str, dict] = {}
        self._id_by_alias: dict[str, str] = {}

    def find_by_id(self, key_id: str) -> dict | None:
        record = self._keys_by_id.get(key_id)
        return self._wire(record) if record is not None else None

    def find_by_alias(self, alias: str) -> dict | None:
        key_id = self._id_by_alias.get(alias)
        return self.find_by_id(key_id) if key_id is not None else None

    def store(
        self, key_id: str, alias: str | None, encapsulation: bytes,
        metadata: Any
    ) -> dict:
        if alias is not None:
            self._ensure_alias_free(alias, key_id)

        existing = self._keys_by_id.get(key_id)
        if existing is not None and existing["alias"] is not None:
            self._id_by_alias.pop(existing["alias"], None)
        if alias is not None:
            self._id_by_alias[alias] = key_id

        record = {
            "id": key_id,
            "alias": alias,
            "encapsulation": encapsulation,
            "metadata": metadata,
        }
        self._keys_by_id[key_id] = record
        return self._wire(record)

    @staticmethod
    def _wire(record: dict) -> dict:
        return {**record, "encapsulation": record["encapsulation"].hex(" ")}

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
    requests. TestKit creates one repository per encryption profile and
    sends its id in NewDriver; the repository is dropped when that driver
    closes. A request naming any other repository id is a backend bug and
    fails the test.
    """

    _stores: ClassVar[dict[str, _Store]] = {}

    @classmethod
    def create(cls):
        """Create a repository and return its id."""
        repository_id = secrets.token_hex(8)
        cls._stores[repository_id] = _Store()
        return repository_id

    @classmethod
    def seed(cls, repository_id, key_id, alias, encapsulation, metadata):
        """Store a pre-existing key directly, without involving the driver."""
        cls._stores[repository_id].store(
            key_id, alias, encapsulation, metadata
        )

    @classmethod
    def forget(cls, repository_id):
        """Drop one repository's storage."""
        cls._stores.pop(repository_id, None)

    @classmethod
    def process_callbacks(cls, request):
        if isinstance(request, EncapsulatedKeyRepositoryFindByIdRequest):
            return cls._find_by_id(request)
        if isinstance(request, EncapsulatedKeyRepositoryFindByAliasRequest):
            return cls._find_by_alias(request)
        if isinstance(request, EncapsulatedKeyRepositoryCreateRequest):
            return cls._create(request)
        if isinstance(request, EncapsulatedKeyRepositorySetAliasRequest):
            return cls._set_alias(request)
        if isinstance(request, EncapsulatedKeyRepositoryDeleteRequest):
            return cls._delete(request)
        return None

    @classmethod
    def _store_for(cls, request) -> _Store:
        store = cls._stores.get(request.repository_id)
        if store is None:
            raise InvalidRepositoryRequestError.unknown_repository(
                request.id, request.repository_id
            )
        return store

    @classmethod
    def _find_by_id(cls, request):
        record = cls._store_for(request).find_by_id(
            request.key_id
        )
        return EncapsulatedKeyRepositoryFindByIdCompleted(request.id, record)

    @classmethod
    def _find_by_alias(cls, request):
        record = cls._store_for(request).find_by_alias(
            request.alias
        )
        return EncapsulatedKeyRepositoryFindByAliasCompleted(
            request.id, record
        )

    @classmethod
    def _create(cls, request):
        encapsulation = cls._decode_encapsulation(request)
        try:
            key_id = secrets.token_hex(8)
            record = cls._store_for(request).store(
                key_id, request.alias, encapsulation, request.metadata
            )
        except _RepositoryError as error:
            return cls._error_completed(request.id, error)

        return EncapsulatedKeyRepositoryCreateCompleted(request.id, record)

    @staticmethod
    def _decode_encapsulation(request):
        try:
            return bytes.fromhex(request.encapsulation)
        except (TypeError, ValueError):
            raise InvalidRepositoryRequestError.invalid_encapsulation(
                request.id, request.encapsulation
            ) from None

    @classmethod
    def _set_alias(cls, request):
        try:
            cls._store_for(request).set_alias(
                request.key_id, request.alias
            )
        except _RepositoryError as error:
            return cls._error_completed(request.id, error)

        return EncapsulatedKeyRepositorySetAliasCompleted(request.id)

    @classmethod
    def _delete(cls, request):
        try:
            cls._store_for(request).delete(request.key_id)
        except _RepositoryError as error:
            return cls._error_completed(request.id, error)

        return EncapsulatedKeyRepositoryDeleteCompleted(request.id)

    @staticmethod
    def _error_completed(request_id, error: _RepositoryError):
        return EncapsulatedKeyRepositoryErrorCompleted(
            request_id, error.error_type, error.detail
        )
