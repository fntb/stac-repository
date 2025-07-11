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
from urllib.parse import urlparse as _urlparse
import posixpath
import hashlib

import orjson

from .git import (
    Repository,
    GitError
)

from ..base_stac_commit import (
    FileNotInRepositoryError,
    JSONObjectError
)
from ..base_stac_transaction import (
    BaseStacTransaction
)

from .git_stac_commit import (
    is_file_not_found_error
)

if TYPE_CHECKING:
    from .git_stac_repository import GitStacRepository


class GitStacTransaction(BaseStacTransaction):

    _repository: "GitStacRepository"
    _git_repository: Repository

    def __init__(self, repository: "GitStacRepository"):
        self._repository = repository
        self._git_repository = repository._git_repository
        self._base_href = repository._base_href

    def _assert_href_in_repository(self, href: str):
        if _urlparse(href, scheme="").scheme != "":
            raise FileNotInRepositoryError(f"{href} is not in repository {self._base_href}")

        href = posixpath.normpath(posixpath.join(self._base_href, href))

        if not href.startswith(self._base_href):
            raise FileNotInRepositoryError(f"{href} is not in repository {self._base_href}")

        return href

    def get(self, href: str) -> Any:
        href = self._assert_href_in_repository(href)
        os_href = os.path.abspath(href)

        try:
            object_str = self._git_repository.show(os_href, text=True)
        except GitError as error:
            if is_file_not_found_error(error):
                raise FileNotFoundError from error
            else:
                raise error

        try:
            return orjson.loads(object_str)
        except orjson.JSONDecodeError as error:
            raise JSONObjectError from error

    def get_asset(self, href: str) -> Iterator[io.RawIOBase | io.BufferedIOBase]:
        href = self._assert_href_in_repository(href)
        os_href = os.path.abspath(href)

        try:
            return self._git_repository.show(os_href, text=False)
        except GitError as error:
            if is_file_not_found_error(error):
                raise FileNotFoundError from error
            else:
                raise error

    def set(self, href: str, value: Any):
        href = self._assert_href_in_repository(href)
        os_href = os.path.abspath(href)

        os.makedirs(os.path.dirname(os_href), exist_ok=True)

        with open(os_href, "w+b") as object_stream:
            try:
                object_stream.write(orjson.dumps(value))
            except orjson.JSONEncodeError as error:
                raise JSONObjectError from error

        self._git_repository.add(os_href)

    def set_asset(self, href: str, value: io.RawIOBase | io.BufferedIOBase):
        href = self._assert_href_in_repository(href)
        os_href = os.path.abspath(href)

        os.makedirs(os.path.dirname(os_href), exist_ok=True)

        with open(os_href, "w+b") as asset_stream:
            while (chunk := value.read()):
                asset_stream.write(chunk)

        self._git_repository.add(os_href)

    def unset(self, href: str):
        href = self._assert_href_in_repository(href)
        os_href = os.path.abspath(href)

        try:
            os.remove(os_href)
        except FileNotFoundError:
            pass

        self._git_repository.remove(os_href)

    def abort(self):
        self._git_repository.reset(clean_modified_files=True)

    def commit(self, *, message: Optional[str] = None):
        if self._git_repository.modified_files:
            modified_files_s = " ".join(self._git_repository.modified_files)
            raise Exception(f"Unexpected unstaged files : {modified_files_s}")

        self._git_repository.commit(message)
