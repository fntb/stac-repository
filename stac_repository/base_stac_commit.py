from __future__ import annotations
from typing import (
    Dict,
    Optional,
    Type,
)

from types import (
    NotImplementedType
)

import logging
import datetime
from abc import abstractmethod, ABCMeta

import pystac

from .stac_catalog import (
    get_child as _get_child,
    ObjectNotFoundError as _ObjectNotFoundError
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
    def get(self, href: str) -> pystac.Item | pystac.Collection | pystac.Catalog:
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
    ) -> pystac.Item | pystac.Collection | pystac.Catalog | None:
        """Searches the object with `id` in the commit catalog.
        """
        try:
            return _get_child(
                id=id,
                href=self._root_catalog_href,
                factory=self.get,
                domain=self._root_catalog_href
            ).object
        except _ObjectNotFoundError:
            return None

    def describe(self):
        raise NotImplementedError
