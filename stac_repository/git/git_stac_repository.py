from __future__ import annotations

from typing import (
    Optional,
    Dict
)

import os
import posixpath

from ..__about__ import __version__, __name_public__

from .git import (
    Repository,
    BareRepository,
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
    RepositoryAlreadyInitializedError,
    RepositoryNotFoundError,
    ConfigError
)
from ..stac import (
    Catalog,
    save
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

    _git_repository: BareRepository
    _lfs_config_file: str
    _base_href: str

    @classmethod
    def init(
        cls,
        repository: str,
        root_catalog: Catalog,
        config: Optional[Dict[str, str]] = None
    ) -> GitStacRepository:
        validated_config = cls.validate_config(config)

        repository_dir = os.path.abspath(repository)
        git_repository = BareRepository(repository_dir)

        if not os.path.isdir(repository_dir):
            os.makedirs(repository_dir, exist_ok=True)

        if os.listdir(repository_dir):
            raise RepositoryAlreadyInitializedError(f"{repository_dir} is not empty")

        if git_repository.is_init:
            raise RepositoryAlreadyInitializedError(f"{repository_dir} is already a git repository")

        git_repository.init()

        with git_repository.tempclone() as concrete_git_repository:
            concrete_git_repository_dir = concrete_git_repository.dir

            gitignore_file = os.path.join(concrete_git_repository_dir, ".gitignore")

            root_catalog.self_href = posixpath.join(posixpath.abspath(concrete_git_repository_dir), "catalog.json")
            save(root_catalog)

            concrete_git_repository.add(os.path.abspath(root_catalog.self_href))

            if validated_config is not None and validated_config.git_lfs_url is not None:
                concrete_git_repository.lfs_url = validated_config.git_lfs_url
                concrete_git_repository.stage_lfs()

            open(gitignore_file, "w").close()
            concrete_git_repository.add(gitignore_file)

            concrete_git_repository.commit("Initialize repository")

        return cls(repository_dir)

    def __init__(
        self,
        repository: str,
    ):
        self._base_href = posixpath.abspath(repository)
        repository_dir = os.path.abspath(self._base_href)

        if not os.path.isdir(repository_dir):
            raise RepositoryNotFoundError

        self._git_repository = BareRepository(repository_dir)

        if not self._git_repository.is_init:
            raise RepositoryNotFoundError(f"{repository_dir} is not a git repository")

    def set_config(
        self,
        config_key: str,
        config_value: str
    ):
        validated_config_value = self.validate_config_option(config_key, config_value)

        with self._git_repository.tempclone() as concrete_git_repository:
            match config_key:
                case "git_lfs_url":
                    concrete_git_repository.lfs_url = validated_config_value
                    concrete_git_repository.stage_lfs()
                case _:
                    raise NotImplementedError

            concrete_git_repository.commit(f"Change configuration option \"{config_key}\"")

    def get_config(self):
        with self._git_repository.tempclone() as concrete_git_repository:
            return GitStacConfig(
                git_lfs_url=concrete_git_repository.lfs_url
            )
