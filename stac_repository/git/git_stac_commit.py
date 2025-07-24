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
import io
import orjson
from contextlib import contextmanager

# from .git import (
#     Commit,
#     Repository,
#     GitError
# )

from .git2 import (
    Commit
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

        if commit is None:
            if repository._local_repository.head is not None:
                self._git_commit = repository._local_repository.head
            else:
                raise RepositoryNotFoundError("Repository doesn't have any commit")
        else:
            self._git_commit = commit

    @property
    def id(self) -> str:
        return self._git_commit.ref

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

    def _href_to_file(self, href: str):
        if not _urlparse(href, scheme="").scheme == "":
            raise HrefError(f"{href} is an external ressource")

        file = os.path.normpath(posixpath.abspath(self._repository._local_repository._repository_dir) + href)

        if not file.startswith(self._repository._local_repository._repository_dir):
            raise HrefError(f"{href} is outside of repository {self._repository._local_repository._repository_dir}")

        return file

    def get(self, href: str) -> Any:
        file = self._href_to_file(href)

        object_str = self._git_commit.read(file)

        try:
            return orjson.loads(object_str)
        except orjson.JSONDecodeError as error:
            raise JSONObjectError from error

    @contextmanager
    def get_asset(self, href: str) -> Iterator[BinaryIO]:
        file = self._href_to_file(href)

        if self._repository._local_repository.is_lfs_installed:
            pointer = self._git_commit.read(file)
            yield io.BytesIO(self._git_commit.lfs_smudge(pointer))
        else:
            yield io.BytesIO(self._git_commit.read(file, text=False))

    def rollback(self):
        return NotImplementedError

        with self._repository._remote_repository.tempclone() as local_repository:
            local_repository.reset(self.id)

        self._repository._local_repository.pull(fetch_lfs_files=False)

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
