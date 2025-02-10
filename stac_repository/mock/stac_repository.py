from typing import Iterator
import datetime

from ..base_stac_repository import BaseStacRepository
from ..base_stac_repository import StacRepositoryConfig

from .stac_commit import StacCommit


class StacRepository(BaseStacRepository):

    def rollback(self, ref: str | datetime.datetime | int):
        raise NotImplementedError

    def backup(self, backup_url: str):
        raise NotImplementedError

    def history(self, stac_object_id: str | None = None) -> Iterator[StacCommit]:
        yield StacCommit(self.catalog_file)
