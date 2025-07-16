from __future__ import annotations
from typing import (
    Optional,
    TYPE_CHECKING
)

from types import (
    NotImplementedType
)

import os
import datetime
import posixpath
from abc import abstractmethod, ABCMeta

from .stac import (
    export,
    Item,
    Collection,
    Catalog,
    ReadableStacIO,
    JSONObjectError,
    FileNotInRepositoryError,
    search
)

if TYPE_CHECKING:
    from .base_stac_repository import BaseStacRepository


class BackupValueError(ValueError):
    ...


class BaseStacCommit(ReadableStacIO, metaclass=ABCMeta):

    @abstractmethod
    def __init__(self, repository: "BaseStacRepository"):
        raise NotImplementedError

    @property
    def _catalog_href(self):
        return posixpath.join(self._base_href, "catalog.json")

    @property
    @abstractmethod
    def id(self) -> str:
        raise NotImplementedError

    @property
    @abstractmethod
    def datetime(self) -> datetime.datetime:
        raise NotImplementedError

    @property
    def message(self) -> str | NotImplementedType:
        return NotImplemented

    @property
    @abstractmethod
    def parent(self) -> BaseStacCommit | None:
        raise NotImplementedError

    def rollback(self) -> Optional[NotImplementedType]:
        """Rollback the repository to this commit.

        Returns:
            NotImplemented: If the concrete implementation does not support rollbacks.
        """
        return NotImplemented

    def backup(self, backup_url: str) -> Optional[NotImplementedType]:
        """Backup the repository as it was in this commit.

        Returns:
            NotImplemented: If the concrete implementation does not support backups.

        Raises:
            BackupValueError: If the backup_url is not valid
        """
        return NotImplemented

    def export(self, export_dir: str):
        """Exports the catalog as it was in this commit.

        Raises:
            FileExistsError
        """

        export(
            self._catalog_href,
            file=os.path.join(os.path.abspath(export_dir), "catalog.json"),
            store=self
        )

    def search(
        self,
        id: str
    ) -> Item | Collection | Catalog | None:
        """Searches the object with `id` in the commit catalog.
        """
        return search(
            self._catalog_href,
            id=id,
            store=self
        )
