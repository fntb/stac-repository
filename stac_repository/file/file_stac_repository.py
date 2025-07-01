from typing import (
    Iterator,
    Optional
)

import os
from os import path

import pystac

from stac_repository.base_stac_repository import (
    BaseStacRepository,
    RepositoryAlreadyInitializedError,
    RepositoryNotFoundError,
)

from .file_stac_commit import FileStacCommit
from .file_stac_transaction import FileStacTransaction


class FileStacRepository(BaseStacRepository):

    _repository_dir: str
    _root_catalog_file: str

    @classmethod
    def init(
        cls,
        repository: str,
        root_catalog: pystac.Catalog,
    ):
        repository_dir = path.abspath(repository)

        if not path.isdir(repository_dir):
            os.makedirs(repository_dir, exist_ok=True)

        if os.listdir(repository_dir):
            raise RepositoryAlreadyInitializedError(f"Repository {repository_dir} is not empty")

        root_catalog.set_self_href(path.join(repository_dir, "catalog.json"))
        root_catalog.save(
            catalog_type=pystac.CatalogType.SELF_CONTAINED,
        )

        return cls(repository_dir)

    def __init__(
        self,
        repository: str
    ):
        self._repository_dir = path.abspath(repository)
        self._root_catalog_file = path.join(self._repository_dir, "catalog.json")

        if not path.exists(self._root_catalog_file):
            raise RepositoryNotFoundError

    @property
    def commits(self) -> Iterator[FileStacCommit]:
        yield FileStacCommit(self)

    def ingest(
        self,
        *sources: str,
        processor_id: Optional[str] = "none",
        parent_id: Optional[str] = None,
    ):
        return super()._ingest(*sources, processor_id=processor_id, parent_id=parent_id, transaction_cls=FileStacTransaction)

    def prune(
        self,
        *product_ids: str,
    ):
        return super()._prune(*product_ids, transaction_cls=FileStacTransaction)
