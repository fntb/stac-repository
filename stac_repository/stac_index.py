from __future__ import annotations
import datetime
import hashlib

import pystac

from .git.git import Repository
from .git.git_stac_io import GitIndexStacIO

from .base_stac_commit import BaseStacCommit
from .stac_commit import StacCommit


class StacIndex(BaseStacCommit):

    _catalog_file: str
    _repository: Repository

    def __init__(self, git_repository: Repository, root_catalog_file: str):
        self._repository = git_repository
        self._catalog_file = root_catalog_file

    @property
    def id(self) -> str:
        return "index"

    @property
    def datetime(self) -> datetime.datetime:
        return datetime.datetime.now()

    @property
    def message(self) -> None:
        return None

    @property
    def parent(self) -> StacCommit | None:
        return StacCommit(
            self._repository.head,
            root_catalog_file=self._catalog_file
        )

    @property
    def catalog(self) -> pystac.Catalog:
        return pystac.Catalog.from_file(self._catalog_file, stac_io=GitIndexStacIO(
            repository=self._repository
        ))

    def fetch(self, href: str, *, text: bool = True, hash: bool = False) -> str | bytes:
        if hash:
            m = hashlib.sha256()
            m.update(self._repository.show(href, text=False))
            return m.hexdigest()
        else:
            return self._repository.show(href, text=text)
