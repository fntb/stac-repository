from __future__ import annotations

from typing import (
    Optional,
    Any,
    Dict
)

import configparser
import urllib.parse
import os
import io
import posixpath
import datetime
from typing import Iterator

from ..__about__ import __version__, __name_public__

from .git import (
    Repository,
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

    _git_repository: Repository
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
        git_repository = Repository(repository_dir)

        if not os.path.isdir(repository_dir):
            os.makedirs(repository_dir, exist_ok=True)

        if os.listdir(repository_dir):
            raise RepositoryAlreadyInitializedError(f"{repository_dir} is not empty")

        if git_repository.is_init:
            raise RepositoryAlreadyInitializedError(f"{repository_dir} is already a git repository")

        git_repository.init()

        if git_repository.modified_files:
            raise RepositoryAlreadyInitializedError(f"{repository_dir} is not empty")

        gitignore_file = os.path.join(repository_dir, ".gitignore")
        gitattributes_file = os.path.join(repository_dir, ".gitattributes")
        lfsconfig_file = os.path.join(repository_dir, ".lfsconfig")

        root_catalog.self_href = posixpath.join(posixpath.abspath(repository_dir), "catalog.json")
        save(root_catalog)

        git_repository.add(os.path.abspath(root_catalog.self_href))

        if validated_config is not None and validated_config.git_lfs_url is not None:
            with open(lfsconfig_file, "w") as file:
                file.write((
                    "[lfs]\n"
                    f"  url = {validated_config.git_lfs_url}"
                ))
        else:
            open(lfsconfig_file, "w").close()

        open(gitignore_file, "w").close()

        with open(gitattributes_file, "w") as file:
            file.write((
                "*\n"
                "!*.json\n"
                "!*/\n"
                "!/.gitignore\n"
                "!/.gitattributes\n"
                "!/.lfsconfig\n"
            ))

        git_repository.add(gitignore_file)
        git_repository.add(lfsconfig_file)
        git_repository.add(gitattributes_file)

        git_repository.commit("Initialize repository")

        return cls(repository_dir)

    def __init__(
        self,
        repository: str,
    ):
        self._base_href = posixpath.abspath(repository)
        repository_dir = os.path.abspath(self._base_href)

        if not os.path.isdir(repository_dir):
            raise RepositoryNotFoundError

        self._git_repository = Repository(repository_dir)

        if not self._git_repository.is_init:
            raise RepositoryNotFoundError(f"{repository_dir} is not a git repository")

        if self._git_repository.modified_files:
            self._git_repository.reset(clean_modified_files=True)

    def set_config(
        self,
        config_key: str,
        config_value: str
    ):
        validated_config_value = self.validate_config_option(config_key, config_value)

        lfsconfig_file = os.path.join(os.path.abspath(self._base_href), ".lfsconfig")

        match config_key:
            case "git_lfs_url":
                lfsconfig_file = os.path.join(os.path.abspath(self._base_href), ".lfsconfig")

                if validated_config_value is not None:
                    with open(lfsconfig_file, "w") as file:
                        file.write((
                            "[lfs]\n"
                            f"  url = {validated_config_value}"
                        ))
                else:
                    open(lfsconfig_file, "w").close()
            case _:
                raise NotImplementedError

        self._git_repository.add(lfsconfig_file)
        self._git_repository.commit(f"Change configuration option \"{config_key}\"")

    def get_config(self):
        lfsconfig_file = os.path.join(os.path.abspath(self._base_href), ".lfsconfig")

        with open(lfsconfig_file, "r") as lfsconfig_stream:
            lfsconfig_str = lfsconfig_stream.read()

        lfsconfig = configparser.ConfigParser()
        lfsconfig.read_string("\n".join([line.strip() for line in lfsconfig_str.splitlines()]))

        try:
            git_lfs_url = lfsconfig["lfs"]["url"]
        except KeyError:
            git_lfs_url = None

        return GitStacConfig(
            git_lfs_url=git_lfs_url
        )
