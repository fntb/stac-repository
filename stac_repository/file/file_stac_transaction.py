from typing import (
    Any,
    Optional,
    Union,
    BinaryIO,
    TYPE_CHECKING
)

from contextlib import contextmanager

import os
import posixpath
import glob
import orjson
from urllib.parse import urlparse as _urlparse

from stac_repository.base_stac_transaction import (
    BaseStacTransaction,
    JSONObjectError,
)

from stac_repository.stac.stac_io import (
    HrefError
)

if TYPE_CHECKING:
    from .file_stac_repository import FileStacRepository


class FileStacTransaction(BaseStacTransaction):

    _base_path: str

    def __init__(self, repository: "FileStacRepository"):
        self._base_path = repository._base_path
        self._lock()

    def _rename_suffixed_files(self, suffix: str):
        for file in glob.iglob(os.path.join(self._base_path, "**", f"*.{suffix}"), recursive=True):
            os.rename(file, file[:-len(f".{suffix}")])

    def _remove_suffixed_files(self, suffix: str):
        for file in glob.iglob(os.path.join(self._base_path, "**", f"*.{suffix}"), recursive=True):
            os.remove(file)

    def _remove_empty_directories(self):
        removed = set()

        for (current_dir, subdirs, files) in os.walk(self._base_path, topdown=False):

            flag = False
            for subdir in subdirs:
                if os.path.join(current_dir, subdir) not in removed:
                    flag = True
                    break

            if not any(files) and not flag:
                os.rmdir(current_dir)
                removed.add(current_dir)

    def _lock(self):
        lock_file = os.path.join(self._base_path, ".lock")

        try:
            with open(lock_file, "r"):
                raise FileExistsError("Cannot lock the repository, another transaction is already taking place.")
        except FileNotFoundError:
            with open(lock_file, "w"):
                os.utime(lock_file, None)

    def _unlock(self):
        lock_file = os.path.join(self._base_path, ".lock")

        try:
            os.remove(lock_file)
        except FileNotFoundError as error:
            raise FileNotFoundError("Cannot unlock the repository.") from error

    def abort(self):
        self._rename_suffixed_files("bck")
        self._remove_suffixed_files("tmp")
        self._remove_empty_directories()
        self._unlock()

    def commit(self, *, message: Optional[str] = None):
        self._rename_suffixed_files("tmp")
        self._remove_suffixed_files("bck")
        self._remove_empty_directories()
        self._unlock()

    def _href_to_file(self, href: str):
        if not _urlparse(href, scheme="").scheme == "":
            raise HrefError(f"{href} is not in repository directory {self._base_path}")

        file = os.path.normpath(posixpath.abspath(self._base_path) + href)

        if not file.startswith(self._base_path):
            raise HrefError(f"{href} is outside of repository {self._base_path}")

        return file

    def get(self, href: str):
        file = self._href_to_file(href)

        try:
            with open(f"{file}.tmp", "r+b") as object_stream:
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
        file = self._href_to_file(href)

        try:
            with open(f"{file}.tmp", "r+b") as asset_stream:
                yield asset_stream
        except FileNotFoundError:
            pass

        with open(file, "r+b") as asset_stream:
            yield asset_stream

    def set(self, href: str, value: Any):
        file = self._href_to_file(href)

        os.makedirs(os.path.dirname(file), exist_ok=True)

        with open(f"{file}.tmp", "w+b") as object_stream:
            try:
                object_stream.write(orjson.dumps(value))
            except orjson.JSONEncodeError as error:
                raise JSONObjectError from error

    def set_asset(self, href: str, value: BinaryIO):
        file = self._href_to_file(href)

        os.makedirs(os.path.dirname(file), exist_ok=True)

        with open(f"{file}.tmp", "w+b") as asset_stream:
            while (chunk := value.read()):
                asset_stream.write(chunk)

    def unset(self, href: str):
        file = self._href_to_file(href)

        try:
            os.rename(file, f"{file}.bck")
        except FileNotFoundError:
            pass

        try:
            os.rename(f"{file}.tmp", f"{file}.bck")
        except FileNotFoundError:
            pass
