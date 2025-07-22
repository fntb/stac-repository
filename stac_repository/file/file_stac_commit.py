from typing import (
    Any,
    Union,
    Optional,
    TYPE_CHECKING
)

from contextlib import contextmanager

import os
import datetime
import shutil
from urllib.parse import urlparse as _urlparse

import orjson

from stac_repository.base_stac_commit import (
    BaseStacCommit,
    BackupValueError,
)

from stac_repository.stac.stac_io import (
    HrefError,
    JSONObjectError
)

if TYPE_CHECKING:
    from .file_stac_repository import FileStacRepository


class FileStacCommit(BaseStacCommit):

    def __init__(self, repository: "FileStacRepository"):
        self._base_href = repository._base_href

    def get(self, href: str):
        if not href.startswith(self._base_href):
            raise HrefError(f"{href} is outside of repository {self._base_href}")

        file = os.path.abspath(href)

        try:
            with open(f"{file}.bck", "r+b") as object_stream:
                try:
                    return orjson.loads(object_stream.read())
                except orjson.JSONDecodeError as error:
                    raise JSONObjectError from error
        except FileNotFoundError:
            pass

        with open(file, "r+b") as object_stream:
            try:
                return orjson.loads(object_stream.read())
            except orjson.JSONDecodeError as error:
                raise JSONObjectError from error

    @contextmanager
    def get_asset(self, href: str):
        if not href.startswith(self._base_href):
            raise HrefError(f"{href} is outside of repository {self._base_href}")

        file = os.path.abspath(href)

        try:
            with open(f"{file}.bck", "r+b") as asset_stream:
                yield asset_stream
        except FileNotFoundError:
            pass

        with open(file, "r+b") as asset_stream:
            yield asset_stream

    @property
    def id(self) -> str:
        return self._base_href

    @property
    def datetime(self) -> datetime.datetime:
        return datetime.datetime.fromtimestamp(os.stat(os.path.abspath(self._base_href)).st_mtime)

    @property
    def message(self):
        return NotImplementedError

    @property
    def parent(self) -> Optional[BaseStacCommit]:
        return None

    def rollback(self):
        pass

    def backup(self, backup_url: str):
        if _urlparse(backup_url).scheme != "":
            raise BackupValueError("Non-filesystem backups are not supported")

        # Replace with rsync
        shutil.copytree(os.path.dirname(self._catalog_href), backup_url, dirs_exist_ok=True)
