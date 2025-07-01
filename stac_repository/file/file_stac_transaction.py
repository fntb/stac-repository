from typing import (
    Dict
)

import os
import shutil
import orjson
import pystac
import glob

from stac_repository.base_stac_transaction import BaseStacTransaction
from stac_repository.base_stac_repository import BaseStacRepository
from .file_stac_commit import FileStacCommit


class FileStacTransaction(FileStacCommit, BaseStacTransaction):

    def __init__(self, repository):
        super().__init__(repository)

        self._lock()

    def _rename_suffixed_files(self, suffix: str):
        root_dir = os.path.dirname(self._root_catalog_href)

        for file in glob.iglob(f"**/*.{suffix}", root_dir=root_dir, recursive=True, include_hidden=True):
            os.rename(os.path.join(root_dir, file), os.path.join(root_dir, file)[:-len(f".{suffix}")])

    def _remove_suffixed_files(self, suffix: str):
        root_dir = os.path.dirname(self._root_catalog_href)

        for file in glob.iglob(f"**/*.{suffix}", root_dir=root_dir, recursive=True, include_hidden=True):
            shutil.rmtree(os.path.join(root_dir, file))

    def _remove_empty_directories(self):
        catalog_dir = os.path.dirname(self._root_catalog_href)

        removed = set()

        for (current_dir, subdirs, files) in os.walk(catalog_dir, topdown=False):

            flag = False
            for subdir in subdirs:
                if os.path.join(current_dir, subdir) not in removed:
                    flag = True
                    break

            if not any(files) and not flag:
                os.rmdir(current_dir)
                removed.add(current_dir)

    def _lock(self):
        catalog_dir = os.path.dirname(self._root_catalog_href)
        lock_file = os.path.join(catalog_dir, ".lock")

        try:
            with open(lock_file, "r"):
                raise FileExistsError("Cannot lock the repository, another transaction is already taking place.")
        except FileNotFoundError:
            with open(lock_file, "w"):
                os.utime(lock_file, None)

    def _unlock(self):
        catalog_dir = os.path.dirname(self._root_catalog_href)
        lock_file = os.path.join(catalog_dir, ".lock")

        try:
            os.remove(lock_file)
        except FileNotFoundError as error:
            raise FileNotFoundError("Cannot unlock the repository.") from error

    def abort(self):
        self._rename_suffixed_files("bck")
        self._remove_suffixed_files("tmp")
        self._remove_empty_directories()
        self._unlock()

    def commit(self, *, message: str | None = None):
        self._rename_suffixed_files("tmp")
        self._remove_suffixed_files("bck")
        self._remove_empty_directories()
        self._unlock()

    def set(self, href: str, value: pystac.Item | pystac.Collection | pystac.Catalog):
        obj_json = value.to_dict(include_self_link=False, transform_hrefs=False)
        obj_s = orjson.dumps(obj_json)

        os.makedirs(os.path.dirname(href), exist_ok=True)

        with open(f"{href}.tmp", "wb") as file:
            file.write(obj_s)

    def unset(self, href: str):
        dir = os.path.dirname(href)
        os.rename(dir, f"{dir}.bck")

    def set_asset(self, href: str, asset_file: str):
        shutil.copyfile(asset_file, f"{href}.tmp")
