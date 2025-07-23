from __future__ import annotations

import hashlib
import tempfile
import os

from ..__about__ import __version__, __name_public__

from .git import (
    Repository,
    BareRepository,
    RemoteRepository,
    RefNotFoundError
)
from .git_stac_commit import (
    GitStacCommit
)
from .git_stac_transaction import (
    GitStacTransaction
)
from .git_stac_config import (
    GitStacConfig
)
from ..base_stac_repository import (
    BaseStacRepository,
    RepositoryNotFoundError,
)


class InvalidBackupUrlError(TypeError):
    pass


class InvalidRollbackRefError(TypeError):
    pass


class RollbackRefNotFoundError(RefNotFoundError):
    pass


class GitStacRepository(BaseStacRepository):

    StacConfig = GitStacConfig
    StacCommit = GitStacCommit
    StacTransaction = GitStacTransaction

    _local_repository: Repository
    _remote_repository: RemoteRepository

    # @classmethod
    # def init(
    #     cls,
    #     root_catalog: Catalog,
    #     config: GitStacConfig
    # ) -> GitStacRepository:
    #     repository_dir = os.path.abspath(config.git_repository)
    #     git_repository = BareRepository(repository_dir)

    #     if not os.path.isdir(repository_dir):
    #         os.makedirs(repository_dir, exist_ok=True)

    #     if os.listdir(repository_dir):
    #         raise RepositoryAlreadyInitializedError(f"{repository_dir} is not empty")

    #     if git_repository.is_init:
    #         raise RepositoryAlreadyInitializedError(f"{repository_dir} is already a git repository")

    #     git_repository.init()

    #     with git_repository.tempclone() as concrete_git_repository:
    #         concrete_git_repository_dir = concrete_git_repository.dir

    #         gitignore_file = os.path.join(concrete_git_repository_dir, ".gitignore")

    #         root_catalog.self_href = posixpath.join(posixpath.abspath(concrete_git_repository_dir), "catalog.json")
    #         save(root_catalog, io=DefaultStacIO({
    #             posixpath.abspath(root_catalog.self_href): StacIOPerm.W_STAC
    #         }))

    #         concrete_git_repository.add(os.path.abspath(root_catalog.self_href))

    #         if config.git_lfs_url is not None:
    #             concrete_git_repository.lfs_url = config.git_lfs_url
    #             concrete_git_repository.stage_lfs()

    #         open(gitignore_file, "w").close()
    #         concrete_git_repository.add(gitignore_file)

    #         concrete_git_repository.commit("Initialize repository")

    #     return cls(repository_dir)

    def __init__(
        self,
        config: GitStacConfig
    ):
        self._remote_repository = RemoteRepository(config.repository)

        if not self._remote_repository.is_init:
            raise RepositoryNotFoundError(f"{config.repository} is not a git repository")

        local_clone_path = os.path.abspath(
            tempfile.tempdir,
            hashlib.sha256(config.repository.encode("utf-8")).hexdigest()
        )

        self._local_repository = self._remote_repository.clone(local_clone_path)
