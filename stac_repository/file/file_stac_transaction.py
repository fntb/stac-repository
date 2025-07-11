from typing import (
    Any,
    TYPE_CHECKING
)

import os
import io
import glob

from stac_repository.base_stac_transaction import (
    BaseStacTransaction,
    JSONObjectError,
    FileNotInRepositoryError
)

from stac_repository.stac.stac_io import (
    DefaultStacIO
)

if TYPE_CHECKING:
    from .file_stac_repository import FileStacRepository


class FileStacTransaction(DefaultStacIO, BaseStacTransaction):

    def __init__(self, repository: "FileStacRepository"):
        self._base_href = repository._base_href
        self._lock()

    def _rename_suffixed_files(self, suffix: str):
        root_dir = os.path.abspath(self._base_href)

        for file in glob.iglob(f"**/*.{suffix}", root_dir=root_dir, recursive=True, include_hidden=True):
            os.rename(os.path.join(root_dir, file), os.path.join(root_dir, file)[:-len(f".{suffix}")])

    def _remove_suffixed_files(self, suffix: str):
        root_dir = os.path.abspath(self._base_href)

        for file in glob.iglob(f"**/*.{suffix}", root_dir=root_dir, recursive=True, include_hidden=True):
            os.remove(os.path.join(root_dir, file))

    def _remove_empty_directories(self):
        root_dir = os.path.abspath(self._base_href)

        removed = set()

        for (current_dir, subdirs, files) in os.walk(root_dir, topdown=False):

            flag = False
            for subdir in subdirs:
                if os.path.join(current_dir, subdir) not in removed:
                    flag = True
                    break

            if not any(files) and not flag:
                os.rmdir(current_dir)
                removed.add(current_dir)

    def _lock(self):
        root_dir = os.path.abspath(self._base_href)
        lock_file = os.path.join(root_dir, ".lock")

        try:
            with open(lock_file, "r"):
                raise FileExistsError("Cannot lock the repository, another transaction is already taking place.")
        except FileNotFoundError:
            with open(lock_file, "w"):
                os.utime(lock_file, None)

    def _unlock(self):
        root_dir = os.path.abspath(self._base_href)
        lock_file = os.path.join(root_dir, ".lock")

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

    def get(self, href: str):
        try:
            return super().get(f"{href}.tmp")
        except FileNotFoundError:
            pass

        return super().get(href)

    def get_asset(self, href: str):
        try:
            return super().get_asset(f"{href}.tmp")
        except FileNotFoundError:
            pass

        return super().get_asset(href)

    def set(self, href: str, value: Any):
        return super().set(f"{href}.tmp", value)

    def unset(self, href: str):
        href = self._assert_href_in_repository(href)
        os_href = os.path.abspath(href)

        try:
            os.rename(os_href, f"{os_href}.bck")
        except FileNotFoundError:
            pass

        try:
            os.rename(f"{os_href}.tmp", f"{os_href}.bck")
        except FileNotFoundError:
            pass

    def set_asset(self, href: str, asset: io.RawIOBase | io.BufferedIOBase):
        return super().set_asset(f"{href}.tmp", asset)
