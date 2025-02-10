from __future__ import annotations
from functools import cached_property
import hashlib
import datetime

import pystac

from .lib.cache import CacheMeta
from .git.git import Commit
from .git.git_stac_io import GitCommitStacIO
from .base_stac_commit import BaseStacCommit


class StacCommit(BaseStacCommit, metaclass=CacheMeta):

    _catalog_file: str
    _commit: Commit

    def __init__(self, git_commit: Commit, root_catalog_file: str):
        self._commit = git_commit
        self._catalog_file = root_catalog_file

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
    def parent(self) -> StacCommit | None:
        return StacCommit(
            self._commit.parent,
            root_catalog_file=self._catalog_file
        ) if self._commit.parent else None

    @property
    def catalog(self) -> pystac.Catalog:
        return pystac.Catalog.from_file(self._catalog_file, stac_io=GitCommitStacIO(
            commit=self._commit
        ))

    def fetch(self, href: str, *, text: bool = True, hash: bool = False) -> str | bytes:
        if hash:
            m = hashlib.sha256()
            m.update(self._commit.show(href, text=False))
            return m.hexdigest()
        else:
            return self._commit.show(href, text=text)

    @cached_property
    def objects(self):
        return super().objects

    @cached_property
    def added_objects(self):
        return super().added_objects

    @cached_property
    def modified_objects(self):
        return super().modified_objects

    @cached_property
    def removed_objects(self):
        return super().removed_objects
