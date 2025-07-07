from __future__ import annotations
from functools import cached_property
import hashlib
import datetime

from ..lib.cache import CacheMeta
from ..lib.git import Commit
from ..base_stac_commit import BaseStacCommit


class GitStacCommit(BaseStacCommit, metaclass=CacheMeta):

    _commit: Commit

    def __init__(self, root_catalog_file: str, git_commit: Commit):
        super().__init__(root_catalog_file)

        self._commit = git_commit

    @cached_property
    def id(self) -> str:
        return self._commit.id

    @cached_property
    def datetime(self) -> datetime.datetime:
        return self._commit.datetime

    @cached_property
    def message(self) -> str:
        return self._commit.message

    @cached_property
    def parent(self) -> GitStacCommit | None:
        return GitStacCommit(
            self._commit.parent,
            root_catalog_file=self._catalog_file
        ) if self._commit.parent else None

    def fetch(self, href: str, *, text: bool = True, hash: bool = False) -> str | bytes:
        if hash:
            m = hashlib.sha256()
            m.update(self._commit.show(href, text=False))
            return m.hexdigest()
        else:
            return self._commit.show(href, text=text)

    @cached_property
    def status(self):
        return super().status
