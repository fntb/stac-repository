from __future__ import annotations

import hashlib
import tempfile
import os

from ..__about__ import __version__, __name_public__

from .git2 import (
    RemoteRepository,
    RefNotFoundError,
    LocalRepository
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

    _local_repository: LocalRepository
    _remote_repository: RemoteRepository

    def __init__(
        self,
        config: GitStacConfig
    ):
        try:
            self._remote_repository = RemoteRepository(config.repository, lfs_url=config.use_lfs)
        except ValueError as error:
            raise RepositoryNotFoundError(f"{config.repository} is not a git repository")

        local_clone_path = os.path.join(
            tempfile.tempdir or os.getcwd(),
            hashlib.sha256(config.repository.encode("utf-8")).hexdigest()
        )

        self._local_repository = self._remote_repository.clone(local_clone_path)
