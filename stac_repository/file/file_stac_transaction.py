from typing import (
    Any,
    Optional,
    Union,
    BinaryIO,
    TYPE_CHECKING
)

from contextlib import contextmanager

import os
import glob
import orjson

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

    def __init__(self, repository: "FileStacRepository"):
        self._base_href = repository._base_href
        self._lock()

    def _rename_suffixed_files(self, suffix: str):
        root_dir = os.path.abspath(self._base_href)

        for file in glob.iglob(os.path.join(root_dir, "**", f"*.{suffix}"), recursive=True):
            os.rename(file, file[:-len(f".{suffix}")])

    def _remove_suffixed_files(self, suffix: str):
        root_dir = os.path.abspath(self._base_href)

        for file in glob.iglob(os.path.join(root_dir, "**", f"*.{suffix}"), recursive=True):
            os.remove(file)

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

    def commit(self, *, message: Optional[str] = None):
        self._rename_suffixed_files("tmp")
        self._remove_suffixed_files("bck")
        self._remove_empty_directories()
        self._unlock()

    def get(self, href: str):
        if not href.startswith(self._base_href):
            raise HrefError(f"{href} is outside of repository {self._base_href}")

        file = os.path.abspath(href)

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
        if not href.startswith(self._base_href):
            raise HrefError(f"{href} is outside of repository {self._base_href}")

        file = os.path.abspath(href)

        try:
            with open(f"{file}.tmp", "r+b") as asset_stream:
                yield asset_stream
        except FileNotFoundError:
            pass

        with open(file, "r+b") as asset_stream:
            yield asset_stream

    def set(self, href: str, value: Any):
        if not href.startswith(self._base_href):
            raise HrefError(f"{href} is outside of repository {self._base_href}")

        file = os.path.abspath(href)

        os.makedirs(os.path.dirname(file), exist_ok=True)

        with open(f"{file}.tmp", "w+b") as object_stream:
            try:
                object_stream.write(orjson.dumps(value))
            except orjson.JSONEncodeError as error:
                raise JSONObjectError from error

    def set_asset(self, href: str, value: BinaryIO):
        if not href.startswith(self._base_href):
            raise HrefError(f"{href} is outside of repository {self._base_href}")

        file = os.path.abspath(href)

        os.makedirs(os.path.dirname(file), exist_ok=True)

        with open(f"{file}.tmp", "w+b") as asset_stream:
            while (chunk := value.read()):
                asset_stream.write(chunk)

    def unset(self, href: str):
        if not href.startswith(self._base_href):
            raise HrefError(f"{href} is outside of repository {self._base_href}")

        file = os.path.abspath(href)

        try:
            os.rename(file, f"{file}.bck")
        except FileNotFoundError:
            pass

        try:
            os.rename(f"{file}.tmp", f"{file}.bck")
        except FileNotFoundError:
            pass
