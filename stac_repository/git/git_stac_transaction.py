from __future__ import annotations

from typing import (
    Any,
    Iterator,
    Optional,
    TYPE_CHECKING
)

from types import (
    NotImplementedType
)

import datetime as datetimelib
import os
import io
import shutil
from urllib.parse import urlparse as _urlparse
import posixpath
import hashlib
from contextlib import contextmanager

import orjson

from .git import (
    Repository,
    BareRepository,
    GitError
)

from ..base_stac_commit import (
    FileNotInRepositoryError,
    JSONObjectError
)
from ..base_stac_transaction import (
    BaseStacTransaction
)

if TYPE_CHECKING:
    from .git_stac_repository import GitStacRepository


class GitStacTransaction(BaseStacTransaction):

    _repository: "GitStacRepository"
    _git_repository: Repository

    def __init__(self, repository: "GitStacRepository"):
        self._repository = repository

        self._git_repository = repository._git_repository.clone()
        self._base_href = repository._base_href

    def _make_concrete_href(self, href: str):
        if _urlparse(href, scheme="").scheme != "":
            raise FileNotInRepositoryError(f"{href} is not in repository {self._base_href}")

        href = posixpath.normpath(posixpath.join(self._base_href, href))

        if not href.startswith(self._base_href):
            raise FileNotInRepositoryError(f"{href} is not in repository {self._base_href}")

        relhref = posixpath.relpath(href, self._base_href)

        return os.path.normpath(os.path.join(self._git_repository.dir, relhref))

    def get(self, href: str) -> Any:
        concrete_href = self._make_concrete_href(href)

        try:
            object_str = self._git_repository.read(concrete_href)
        except GitError as error:
            if GitError.is_file_not_found_error(error):
                raise FileNotFoundError from error
            else:
                raise error

        try:
            return orjson.loads(object_str)
        except orjson.JSONDecodeError as error:
            raise JSONObjectError from error

    @contextmanager
    def get_asset(self, href: str) -> Iterator[io.RawIOBase | io.BufferedIOBase]:
        concrete_href = self._make_concrete_href(href)

        try:
            yield self._git_repository.smudge(concrete_href)
        except GitError as error:
            if GitError.is_file_not_found_error(error):
                raise FileNotFoundError from error
            else:
                raise error

    def set(self, href: str, value: Any):
        concrete_href = self._make_concrete_href(href)

        os.makedirs(os.path.dirname(concrete_href), exist_ok=True)

        with open(concrete_href, "w+b") as object_stream:
            try:
                object_stream.write(orjson.dumps(value))
            except orjson.JSONEncodeError as error:
                raise JSONObjectError from error

        self._git_repository.add(concrete_href)

    def set_asset(self, href: str, value: io.RawIOBase | io.BufferedIOBase):
        concrete_href = self._make_concrete_href(href)

        if posixpath.splitext(_urlparse(href).path)[1] != ".json":
            self._git_repository.lfs_track(concrete_href)
            self._git_repository.stage_lfs()

        os.makedirs(os.path.dirname(concrete_href), exist_ok=True)

        with open(concrete_href, "w+b") as asset_stream:
            while (chunk := value.read()):
                asset_stream.write(chunk)

        self._git_repository.add(concrete_href)

    def unset(self, href: str):
        concrete_href = self._make_concrete_href(href)

        try:
            os.remove(concrete_href)
        except FileNotFoundError:
            pass

        self._git_repository.remove(concrete_href)

    def abort(self):
        self._git_repository.reset(clean_modified_files=True)

        shutil.rmtree(self._git_repository.dir, ignore_errors=True)

    def commit(self, *, message: Optional[str] = None):
        if self._git_repository.modified_files:
            modified_files_s = " ".join(self._git_repository.modified_files)
            raise Exception(f"Unexpected unstaged files : {modified_files_s}")

        self._git_repository.commit(message)
        self._git_repository.push()
