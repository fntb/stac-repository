from __future__ import annotations
import urllib.parse
import os
from os import path
import urllib
import datetime
from typing import Iterator

import pystac
import pystac.layout

from .__about__ import __version__, __name_public__

from .git.git import Repository, RefNotFoundError
from .stac_commit import StacCommit
from .base_stac_repository import (
    StacRepositoryConfig,
    BaseStacRepository,
    catalog_file_name,
    RepositoryAlreadyInitializedError,
    RepositoryNotFoundError
)


class InvalidBackupUrlError(TypeError):
    pass


class InvalidRollbackRefError(TypeError):
    pass


class RollbackRefNotFoundError(RefNotFoundError):
    pass


class StacRepository(BaseStacRepository):

    _repository: Repository

    @classmethod
    def init(
        cls,
        repository_dir: str,
        *,
        catalog_config: StacRepositoryConfig = StacRepositoryConfig(),
        git_lfs_url: str | None = None,
        git_lfs_filter: list[str] | None = [
            "*.tar.bz2", "*.tar.xz", "*.tar.gz", "*.tar", "*.zip", "*.bin"],
        ignore: list[str] = [".cache"],
    ):
        repository_dir = path.abspath(repository_dir)
        catalog_file = path.join(repository_dir, catalog_file_name)
        uses_git_lfs = git_lfs_url is not None

        repository = Repository(repository_dir)

        if not path.isdir(repository_dir):
            os.makedirs(repository_dir, exist_ok=True)

        if os.listdir(repository_dir):
            raise RepositoryAlreadyInitializedError(f"Repository {repository_dir} is not empty")

        if repository.is_init:
            raise RepositoryAlreadyInitializedError(f"Repository {repository_dir} is already already a git repository")

        repository.init()

        if repository.modified_files:
            raise RepositoryAlreadyInitializedError(f"Repository {repository_dir} is not empty")

        gitignore_file = path.join(repository_dir, ".gitignore")
        gitattributes_file = path.join(repository_dir, ".gitattributes")
        lfsconfig_file = path.join(repository_dir, ".lfsconfig")

        catalog = pystac.Catalog(
            catalog_config.id,
            catalog_config.description,
            title=catalog_config.title,
            catalog_type=pystac.CatalogType.SELF_CONTAINED,
            strategy=pystac.layout.BestPracticesLayoutStrategy(),
        )
        catalog.set_self_href(catalog_file)
        catalog.save()

        repository.add(catalog_file)

        with open(gitignore_file, "w") as file:
            for ignored in ignore:
                file.write(f"{ignored}\n")

        repository.add(
            gitignore_file
        )

        if uses_git_lfs:

            with open(gitattributes_file, "w") as file:
                for filter in git_lfs_filter or []:
                    file.write(
                        f"{filter} filter=lfs diff=lfs merge=lfs -text\n"
                    )

            with open(lfsconfig_file, "w") as file:
                file.write(f"[lfs]\n    url = {git_lfs_url}\n\n")

            repository.add(
                gitattributes_file,
                lfsconfig_file
            )

        repository.commit("Initial commit")

        return cls(repository_dir)

    def __init__(
        self,
        repository_dir: str,
    ):
        super().__init__(repository_dir)

        if not path.isdir(self._dir):
            raise RepositoryNotFoundError

        if not path.isfile(self._catalog_file):
            raise RepositoryNotFoundError

        self._repository = Repository(repository_dir)

        if not self._repository.is_init:
            raise RepositoryNotFoundError

        if self._repository.modified_files:
            raise RepositoryNotFoundError

    def rollback(self, ref: str | datetime.datetime | int):
        repository = self._repository

        id = None

        if isinstance(ref, str):
            commit = repository[ref]
            if commit is None:
                raise RollbackRefNotFoundError
            id = commit.id
        elif isinstance(ref, int) or (isinstance(ref, float) and int(ref) == ref):
            commit = repository.head
            while ref > 0:
                commit = commit.parent
                if commit == None:
                    raise RollbackRefNotFoundError
                ref -= 1
            id = commit.id
        elif isinstance(ref, datetime.datetime):
            commit = repository.head
            while commit.datetime > ref:
                commit = commit.parent
                if commit == None:
                    raise RollbackRefNotFoundError
            id = commit.id
        else:
            raise InvalidRollbackRefError

        repository.reset(id)

    def backup(self, backup_url: str):

        mode = urllib.parse.urlparse(backup_url, "file").scheme

        if mode == "file":
            backup_dir = backup_url
            backup_repository = Repository(backup_dir)

            if backup_repository.is_init:
                backup_repository.pull()
            else:
                backup_repository.clone(self._repository.dir)
        elif mode == "ssh":
            # Remote : https://stackoverflow.com/a/19071079
            raise NotImplementedError
        else:
            raise InvalidBackupUrlError

    def history(self, object_id: str | None = None) -> Iterator[StacCommit]:
        if self._repository.head is None:
            return

        commit = StacCommit(
            self._repository.head,
            root_catalog_file=self.catalog_file
        )

        while commit is not None:
            if object_id is None:
                yield commit
                commit = commit.parent
            else:
                for object in commit.objects:
                    if object.id == object_id:
                        yield commit
                        break
                else:
                    for object in commit.deleted_objects:
                        if object.id == object_id:
                            yield commit
                            break
                commit = commit.parent

    def ingest_products(self, processor_id, *product_sources, transaction_type: StacIngestTransaction = StacIngestTransaction):
        return super().ingest_products(processor_id, *product_sources, transaction_type=StacIngestTransaction)

    def ingest(self, processor_id, source):
        return super().ingest(processor_id, source, transaction_type=StacIngestTransaction)

    def prune(self, *product_ids):
        return super().prune(*product_ids, transaction_type=StacPruneTransaction)
