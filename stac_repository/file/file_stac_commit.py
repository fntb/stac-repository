from typing import (
    Any,
    TYPE_CHECKING
)

from types import (
    NotImplementedType
)

import os
import io
import glob
import posixpath
import datetime
import shutil
from urllib.parse import urlparse as _urlparse

from stac_repository.base_stac_transaction import (
    BaseStacTransaction,
    JSONObjectError,
    FileNotInRepositoryError
)

from stac_repository.base_stac_commit import (
    BaseStacCommit,
    BackupValueError
)

from stac_repository.stac.stac_io import (
    DefaultStacIO,
    DefaultReadableStacIO
)

if TYPE_CHECKING:
    from .file_stac_repository import FileStacRepository


class FileStacCommit(DefaultReadableStacIO, BaseStacCommit):

    def __init__(self, repository: "FileStacRepository"):
        self._base_href = repository._base_href

    def get(self, href: str):
        try:
            return super().get(f"{href}.bck")
        except FileNotFoundError:
            pass

        return super().get(href)

    def get_asset(self, href: str):
        try:
            return super().get_asset(f"{href}.bck")
        except FileNotFoundError:
            pass

        return super().get_asset(href)

    @property
    def id(self) -> str:
        return self._base_href

    @property
    def datetime(self) -> datetime.datetime:
        return datetime.datetime.fromtimestamp(os.stat(os.path.abspath(self._base_href)).st_mtime)

    @property
    def message(self):
        return NotImplemented

    @property
    def parent(self) -> BaseStacCommit | None:
        return None

    def rollback(self):
        pass

    def backup(self, backup_url: str):
        if _urlparse(backup_url).scheme != "":
            raise BackupValueError("Non-filesystem backups are not supported")

        # Replace with rsync
        shutil.copytree(os.path.dirname(self._root_catalog_href), backup_url, dirs_exist_ok=True)
