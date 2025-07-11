
import os
import posixpath

from stac_repository.base_stac_repository import (
    BaseStacRepository,
    RepositoryAlreadyInitializedError,
    RepositoryNotFoundError,
)

from stac_repository.stac import (
    Catalog,
    save
)

from .file_stac_transaction import FileStacTransaction
from .file_stac_commit import FileStacCommit


class FileStacRepository(BaseStacRepository):

    StacTransaction = FileStacTransaction
    StacCommit = FileStacCommit

    _base_href: str

    @classmethod
    def init(
        cls,
        repository: str,
        root_catalog: Catalog,
    ):
        repository_dir = os.path.abspath(repository)

        if not os.path.isdir(repository_dir):
            os.makedirs(repository_dir, exist_ok=True)

        if os.listdir(repository_dir):
            raise RepositoryAlreadyInitializedError(f"Repository {repository_dir} is not empty")

        root_catalog.self_href = posixpath.join(posixpath.abspath(repository_dir), "catalog.json")
        save(root_catalog)

        return cls(repository_dir)

    def __init__(
        self,
        repository: str
    ):
        self._base_href = posixpath.abspath(repository)

        if not os.path.exists(os.path.abspath(self._base_href)):
            raise RepositoryNotFoundError
