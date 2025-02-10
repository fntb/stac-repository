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

from .git.git import Repository, Signature, RefNotFoundError
from .stac_commit import StacCommit
from .base_stac_repository import BaseStacRepository
from .base_stac_repository import StacRepositoryConfig


class InvalidBackupUrlError(TypeError):
    pass


class InvalidRollbackRefError(TypeError):
    pass


class RollbackRefNotFoundError(RefNotFoundError):
    pass


class UncleanRepositoryDirectory(FileExistsError):
    pass


class StacRepository(BaseStacRepository):

    _uses_git_lfs: bool
    _signature: Signature
    _repository: Repository

    def __init__(
            self,
            repository_dir: str,
            *,
            catalog_file: str = pystac.Catalog.DEFAULT_FILE_NAME,
            catalog_config: StacRepositoryConfig = StacRepositoryConfig(
                id="root"
            ),
            git_lfs_url: str | None = None,
            git_lfs_filter: list[str] | None = [
                "*.tar.bz2", "*.tar.xz", "*.tar.gz", "*.tar", "*.zip", "*.bin"],
            ignore: list[str] = [".cache"],
            signature: Signature | str = Signature(
                name=f"{__name_public__}:{__version__}",
            )
    ):
        self._dir = path.abspath(repository_dir)
        self._catalog_file = path.join(self.dir, catalog_file)
        self._uses_git_lfs = git_lfs_url is not None
        if isinstance(signature, str):
            self._signature = Signature.make(signature)
        else:
            self._signature = signature
        self._repository = Repository(repository_dir)

        if not path.isdir(self.dir):
            os.makedirs(self.dir, exist_ok=True)

        if not self._repository.is_init:
            self._repository.init()

        gitignore_file = path.join(repository_dir, ".gitignore")
        gitattributes_file = path.join(repository_dir, ".gitattributes")
        lfsconfig_file = path.join(repository_dir, ".lfsconfig")

        if not self._repository.head:

            if self._repository.modified_files:
                raise UncleanRepositoryDirectory

            catalog = pystac.Catalog(
                catalog_config["id"],
                catalog_config.get("description", None) or "",
                title=catalog_config.get("title", None) or "",
                catalog_type=pystac.CatalogType.SELF_CONTAINED,
                strategy=pystac.layout.BestPracticesLayoutStrategy(),
            )
            catalog.set_self_href(self.catalog_file)
            catalog.save()

            self._repository.add(
                self.catalog_file
            )

            with open(gitignore_file, "w") as file:
                for ignored in ignore:
                    file.write(f"{ignored}\n")

            self._repository.add(
                gitignore_file
            )

            if self._uses_git_lfs:

                with open(gitattributes_file, "w") as file:
                    for filter in git_lfs_filter or []:
                        file.write(
                            f"{filter} filter=lfs diff=lfs merge=lfs -text\n"
                        )

                with open(lfsconfig_file, "w") as file:
                    file.write(f"[lfs]\n    url = {git_lfs_url}\n\n")

                self._repository.add(
                    gitattributes_file,
                    lfsconfig_file
                )

            self._repository.commit(
                "Initial commit",
                self.signature,
                self.signature
            )

    @property
    def signature(self) -> Signature:
        """Committer signature"""
        return self._signature

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
