from __future__ import annotations
from typing import (
    Dict,
    Optional,
    Type,
)

from types import (
    NotImplementedType
)

import os
import logging
import datetime
import io
import orjson
from abc import abstractmethod, ABCMeta

import pystac
import pystac.stac_io

from .stac_catalog import (
    get_child as _get_child,
    ObjectNotFoundError as _ObjectNotFoundError,
    make_from_str as _make_from_str
)


class BackupValueError(ValueError):
    ...


class CommitStacIOWriteAttemptError(NotImplementedError):
    ...


class CommitStacIO(pystac.stac_io.DefaultStacIO):

    _commit: BaseStacCommit

    def __init__(self, *args: pystac.Any, commit: BaseStacCommit, **kwargs: pystac.Any):
        super().__init__(*args, **kwargs)
        self._commit = commit

    def read_text_from_href(self, href: str) -> str:
        return self._commit.get(href)

    def write_text_to_href(self, href: str, txt: str) -> None:
        raise CommitStacIOWriteAttemptError


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
    def get(self, href: str) -> str:
        raise NotImplementedError

    @abstractmethod
    def get_asset(self, href: str) -> io.BytesIO:
        raise NotImplementedError

    @property
    def io(self) -> CommitStacIO:
        return CommitStacIO(commit=self)

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
                stac_io=self.io,
                domain=os.path.dirname(self._root_catalog_href)
            ).object
        except _ObjectNotFoundError:
            return None

    def describe(self):
        raise NotImplementedError
