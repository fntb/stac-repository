from __future__ import annotations

from typing import (
    Optional,
    Any,
    Iterator,
    BinaryIO,
    TYPE_CHECKING
)

import datetime
import posixpath
from urllib.parse import urlparse as _urlparse
import os
import orjson
from contextlib import contextmanager

from .git import (
    Commit,
    Repository,
    GitError
)
from ..base_stac_commit import (
    BaseStacCommit,
    JSONObjectError,
    BackupValueError,
    HrefError
)

if TYPE_CHECKING:
    from .git_stac_repository import (
        GitStacRepository,
        RepositoryNotFoundError
    )


class GitStacCommit(BaseStacCommit):

    _git_commit: Commit
    _repository: "GitStacRepository"

    def __init__(self, repository: "GitStacRepository", commit: Optional[Commit] = None):
        self._repository = repository
        self._base_href = repository._base_href

        if commit is None:
            if repository._git_repository.head is not None:
                self._git_commit = repository._git_repository.head
            else:
                raise RepositoryNotFoundError("Repository doesn't have any commit")
        else:
            self._git_commit = commit

    @property
    def id(self) -> str:
        return self._git_commit.id

    @property
    def datetime(self) -> datetime.datetime:
        return self._git_commit.datetime

    @property
    def message(self) -> str:
        return self._git_commit.message

    @property
    def parent(self) -> Optional[GitStacCommit]:
        return GitStacCommit(
            self._repository,
            self._git_commit.parent
        ) if self._git_commit.parent else None

    def _assert_href_in_repository(self, href: str):
        if _urlparse(href, scheme="").scheme != "":
            raise HrefError(f"{href} is not in repository {self._base_href}")

        href = posixpath.normpath(posixpath.join(self._base_href, href))

        if not href.startswith(self._base_href):
            raise HrefError(f"{href} is not in repository {self._base_href}")

        return href

    def get(self, href: str) -> Any:
        href = self._assert_href_in_repository(href)
        os_href = os.path.abspath(href)

        try:
            object_str = self._git_commit.read(os_href)
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
    def get_asset(self, href: str) -> Iterator[BinaryIO]:
        href = self._assert_href_in_repository(href)
        os_href = os.path.abspath(href)

        try:
            yield self._git_commit.smudge(os_href)
        except GitError as error:
            if GitError.is_file_not_found_error(error):
                raise FileNotFoundError from error
            else:
                raise error

    def rollback(self):
        with self._repository._git_repository.tempclone() as concrete_git_repository:
            concrete_git_repository.reset(self.id)

    def backup(self, backup_url: str):
        return NotImplementedError

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
