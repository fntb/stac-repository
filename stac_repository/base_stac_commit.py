from __future__ import annotations
from typing import (
    Dict,
    Optional,
    Type,
    Any
)

from types import (
    NotImplementedType
)

import os
import datetime
import io
from abc import abstractmethod, ABCMeta

from .stac import (
    search,
    Item,
    Collection,
    Catalog
)


class BackupValueError(ValueError):
    ...


class BaseStacCommit(metaclass=ABCMeta):

    _root_catalog_href: str

    @abstractmethod
    def __init__(self, repository):
        raise NotImplementedError

    @property
    @abstractmethod
    def id(self) -> str:
        raise NotImplementedError

    @property
    @abstractmethod
    def datetime(self) -> datetime.datetime:
        raise NotImplementedError

    @property
    @abstractmethod
    def message(self) -> Optional[str]:
        raise NotImplementedError

    @abstractmethod
    def get(self, href: str) -> Any:
        raise NotImplementedError

    @abstractmethod
    def get_asset(self, href: str) -> io.RawIOBase | io.BufferedIOBase:
        raise NotImplementedError

    @abstractmethod
    def rollback(self) -> Optional[NotImplementedType]:
        """Rollback the repository to this commit.

        Returns:
            NotImplemented: If the concrete implementation does not support rollbacks.
        """
        raise NotImplementedError

    @abstractmethod
    def backup(self, backup_url: str) -> Optional[NotImplementedType]:
        """Backup the repository as it was in this commit.

        Returns:
            NotImplemented: If the concrete implementation does not support backups.

        Raises:
            BackupValueError: If the backup_url is not valid
        """
        raise NotImplementedError

    def search(
        self,
        id: str
    ) -> Item | Collection | Catalog | None:
        """Searches the object with `id` in the commit catalog.
        """
        return search(
            self._root_catalog_href,
            id=id,
            scope=os.path.dirname(self._root_catalog_href),
            store=self
        )

    def describe(self):
        raise NotImplementedError
