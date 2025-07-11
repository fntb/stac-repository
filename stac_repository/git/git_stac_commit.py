from __future__ import annotations

from typing import (
    Optional,
    Any,
    Iterator,
    TYPE_CHECKING
)

from functools import cached_property
import hashlib
import datetime
import posixpath
from urllib.parse import urlparse as _urlparse
import os
import orjson
import io
import re

from .git import (
    Commit,
    Repository,
    GitError
)
from ..base_stac_commit import (
    BaseStacCommit,
    FileNotInRepositoryError,
    JSONObjectError,
    BackupValueError
)

if TYPE_CHECKING:
    from .git_stac_repository import GitStacRepository


def is_file_not_found_error(error: GitError) -> bool:
    return re.search(r"path.+does not exist", str(error))


class GitStacCommit(BaseStacCommit):

    _git_commit: Commit
    _repository: "GitStacRepository"

    def __init__(self, repository: "GitStacRepository", commit: Optional[Commit] = None):
        self._repository = repository
        self._base_href = repository._base_href

        if commit is None:
            commit = repository._git_repository.head

        self._git_commit = commit

    @cached_property
    def id(self) -> str:
        return self._git_commit.id

    @cached_property
    def datetime(self) -> datetime.datetime:
        return self._git_commit.datetime

    @cached_property
    def message(self) -> str:
        return self._git_commit.message

    @cached_property
    def parent(self) -> GitStacCommit | None:
        return GitStacCommit(
            self._repository,
            self._git_commit.parent
        ) if self._git_commit.parent else None

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
            object_str = self._git_commit.show(os_href, text=True)
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
            return self._git_commit.show(os_href, text=False)
        except GitError as error:
            if is_file_not_found_error(error):
                raise FileNotFoundError from error
            else:
                raise error

    def rollback(self):
        self._repository._git_repository.reset(self.id)

    def backup(self, backup_url: str):

        # Backup by ref
        return NotImplemented

        mode = _urlparse(backup_url, "file").scheme

        if mode == "file":
            backup_dir = backup_url
            backup_repository = Repository(backup_dir)

            if backup_repository.is_init:
                backup_repository.pull()
            else:
                backup_repository.clone(os.path.abspath(self._repository._base_href))
        elif mode == "ssh":
            # Remote : https://stackoverflow.com/a/19071079
            raise NotImplementedError
        else:
            raise BackupValueError
