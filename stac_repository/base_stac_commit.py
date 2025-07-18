from __future__ import annotations
from typing import (
    Optional,
    TYPE_CHECKING,
    Union,
    Type,
    Literal
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
    def message(self) -> Union[str, Type[NotImplementedError]]:
        return NotImplementedError

    @property
    @abstractmethod
    def parent(self) -> Optional[BaseStacCommit]:
        raise NotImplementedError

    def rollback(self) -> Optional[Type[NotImplementedError]]:
        """Rollback the repository to this commit.

        Returns:
            NotImplementedError: If the concrete implementation does not support rollbacks.
        """
        return NotImplementedError

    def backup(self, backup_url: str) -> Optional[Type[NotImplementedError]]:
        """Backup the repository as it was in this commit.

        Returns:
            NotImplementedError: If the concrete implementation does not support backups.

        Raises:
            BackupValueError: If the backup_url is not valid
        """
        return NotImplementedError

    def export(self, export_dir: str):
        """Exports the catalog as it was in this commit.

        Raises:
            FileExistsError
        """

        export(
            self._catalog_href,
            file=os.path.join(os.path.abspath(export_dir), "catalog.json"),
        )

    def search(
        self,
        id: str
    ) -> Optional[Union[Item, Collection, Catalog]]:
        """Searches the object with `id` in the commit catalog.
        """
        return search(
            self._catalog_href,
            id=id,
            store=self
        )
