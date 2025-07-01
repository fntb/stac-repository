from __future__ import annotations
import datetime as datetimelib
import os
import hashlib

from ..lib.git import Repository

from ..base_stac_commit import BaseStacCommit
from .git_stac_commit import GitStacCommit


class GitStacIndex(BaseStacCommit):

    _repository: Repository

    def __init__(self, root_catalog_file: str, git_repository: Repository):
        super().__init__(root_catalog_file)

        self._repository = git_repository

    @property
    def id(self) -> str:
        return "index"

    @property
    def datetime(self) -> datetime.datetime:
        return datetimelib.datetime.fromtimestamp(os.stat(os.path.dirname(self._catalog_file)).st_mtime)

    @property
    def message(self) -> None:
        return None

    @property
    def parent(self) -> GitStacCommit | None:
        return GitStacCommit(
            self._catalog_file,
            self._repository.head,
        )

    def fetch(self, href: str, *, text: bool = True, hash: bool = False) -> str | bytes:
        if hash:
            m = hashlib.sha256()
            m.update(self._repository.show(href, text=False))
            return m.hexdigest()
        else:
            return self._repository.show(href, text=text)
