from __future__ import annotations
from typing import (
    Optional,
    TYPE_CHECKING,
    Union,
    Type,
)


import datetime
import posixpath
from abc import abstractmethod, ABCMeta

from .stac import (
    Item,
    Collection,
    Catalog,
    ReadableStacIO,
    search,
    StacObjectError,
    JSONObjectError,
    HrefError
)

if TYPE_CHECKING:
    from .base_stac_repository import BaseStacRepository


class BackupValueError(ValueError):
    """Backend cannot process this type of backup destination."""
    pass


class BaseStacCommit(ReadableStacIO, metaclass=ABCMeta):

    @abstractmethod
    def __init__(self, repository: "BaseStacRepository"):
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
        """
        raise NotImplementedError

    def search(
        self,
        id: str,
    ) -> Optional[Union[Item, Collection, Catalog]]:
        """Searches the cataloged object `id`.

        This method will **not** lookup objects outside of the repository.
        """
        return search(
            "/catalog.json",
            id=id,
            io=self,
        )
