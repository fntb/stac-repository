from __future__ import annotations
import os
from os import path
import datetime
from typing import Iterator, TypedDict, Optional
import abc

import pystac
import pystac.layout

from .base_stac_commit import BaseStacCommit


class StacRepositoryConfig(TypedDict):
    id: str
    title: Optional[str | None]
    description: Optional[str | None]


class BaseStacRepository(metaclass=abc.ABCMeta):

    _dir: str
    _catalog_file: str

    def __init__(
            self,
            repository_dir: str,
            *,
            catalog_file: str = pystac.Catalog.DEFAULT_FILE_NAME,
            catalog_config: StacRepositoryConfig = StacRepositoryConfig(
                id="root"
            )
    ):
        self._dir = path.abspath(repository_dir)
        self._catalog_file = path.join(self.dir, catalog_file)

        if not path.isdir(self.dir):
            os.makedirs(self.dir, exist_ok=True)

        if not os.listdir(self.dir):
            catalog = pystac.Catalog(
                catalog_config["id"],
                catalog_config.get("description", None) or "",
                title=catalog_config.get("title", None) or "",
                catalog_type=pystac.CatalogType.SELF_CONTAINED,
                strategy=pystac.layout.BestPracticesLayoutStrategy(),
            )
            catalog.set_self_href(self.catalog_file)
            catalog.save()

    @property
    def dir(self) -> str:
        """Repository directory"""
        return self._dir

    @property
    def catalog_file(self) -> str:
        """Catalog file"""
        return self._catalog_file

    @abc.abstractmethod
    def rollback(self, ref: str | datetime.datetime | int):
        raise NotImplementedError

    @abc.abstractmethod
    def backup(self, backup_url: str):
        raise NotImplementedError

    @abc.abstractmethod
    def history(self, stac_object_id: str | None = None) -> Iterator[BaseStacCommit]:
        raise NotImplementedError
