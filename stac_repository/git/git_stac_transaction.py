from __future__ import annotations

from typing import (
    Any,
    Iterator,
    Optional,
    Union,
    BinaryIO,
    TYPE_CHECKING
)

import os
import io
import shutil
from urllib.parse import urlparse as _urlparse
import posixpath
from contextlib import contextmanager

import orjson

from .git2 import (
    LocalRepository
)

from ..base_stac_commit import (
    JSONObjectError,
    HrefError
)
from ..base_stac_transaction import (
    BaseStacTransaction
)

if TYPE_CHECKING:
    from .git_stac_repository import GitStacRepository


class GitStacTransaction(BaseStacTransaction):

    _repository: "GitStacRepository"
    _git_repository: LocalRepository

    def __init__(self, repository: "GitStacRepository"):
        self._repository = repository

        self._git_repository = repository._local_repository

    def _href_to_file(self, href: str):
        if not _urlparse(href, scheme="").scheme == "":
            raise HrefError(f"{href} is an external ressource")

        file = os.path.normpath(posixpath.abspath(self._git_repository._repository_dir) + href)

        if not file.startswith(self._git_repository._repository_dir):
            raise HrefError(f"{href} is outside of repository {self._git_repository._repository_dir}")

        return file

    def get(self, href: str) -> Any:
        file = self._href_to_file(href)

        object_str = self._git_repository.read(file)

        try:
            return orjson.loads(object_str)
        except orjson.JSONDecodeError as error:
            raise JSONObjectError from error

    @contextmanager
    def get_asset(self, href: str) -> Iterator[BinaryIO]:
        file = self._href_to_file(href)

        if self._repository._local_repository.is_lfs_installed:
            pointer = self._git_repository.read(file)
            yield io.BytesIO(self._git_repository.lfs_smudge(pointer))
        else:
            yield io.BytesIO(self._git_repository.read(file, text=False))

    def set(self, href: str, value: Any):
        file = self._href_to_file(href)

        os.makedirs(os.path.dirname(file), exist_ok=True)

        with open(file, "w+b") as object_stream:
            try:
                object_stream.write(orjson.dumps(value))
            except orjson.JSONEncodeError as error:
                raise JSONObjectError from error

        self._git_repository.add(file)

    def set_asset(self, href: str, value: BinaryIO):
        file = self._href_to_file(href)

        if self._git_repository.is_lfs_installed:
            file_name = os.path.relpath(file, self._git_repository._repository_dir)
            self._git_repository.lfs_track(file_name)

        os.makedirs(os.path.dirname(file), exist_ok=True)

        with open(file, "w+b") as asset_stream:
            while (chunk := value.read()):
                asset_stream.write(chunk)

        self._git_repository.add(file)

    def unset(self, href: str):
        file = self._href_to_file(href)

        try:
            os.remove(file)
            self._git_repository.remove(file)
        except FileNotFoundError:
            pass

    def abort(self):
        self._git_repository.reset(clean_modified_files=True)

    def commit(self, *, message: Optional[str] = None):
        if self._git_repository.modified_files:
            modified_files_s = " ".join(self._git_repository.modified_files)
            raise Exception(f"Unexpected unstaged files : {modified_files_s}")

        self._git_repository.commit(message or "")
        self._git_repository.push()
