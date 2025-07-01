from typing import (
    Dict
)

from types import (
    NotImplementedType
)

import os
import datetime
import urllib.parse
import shutil

import pystac

from stac_repository.base_stac_commit import (
    BaseStacCommit,
    BackupValueError
)


class FileStacCommit(BaseStacCommit):

    def __init__(self, repository):
        self._root_catalog_href = repository._root_catalog_file

    @property
    def id(self) -> str:
        return os.path.dirname(self._root_catalog_href)

    @property
    def datetime(self) -> datetime.datetime:
        return datetime.datetime.fromtimestamp(os.stat(os.path.dirname(self._root_catalog_href)).st_mtime)

    @property
    def message(self) -> None:
        return None

    def get(self, href: str) -> pystac.Item | pystac.Catalog:
        for cls in (pystac.Item, pystac.Collection, pystac.Catalog):
            try:
                return cls.from_file(href)
            except pystac.STACTypeError:
                pass

        raise ValueError(f"{href} is not a valid STAC object")

    def rollback(self) -> NotImplementedType:
        return NotImplemented

    def backup(self, backup_dir: str):
        if urllib.parse.urlparse(backup_dir).scheme != "":
            raise BackupValueError("Non-filesystem backups are not supported")

        # Replace with rsync
        shutil.copytree(os.path.dirname(self._root_catalog_href), backup_dir, dirs_exist_ok=True)
