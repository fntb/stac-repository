import os
import posixpath

from pydantic import (
    BaseModel,
    Field
)

from stac_repository.base_stac_repository import (
    BaseStacRepository,
)

from .file_stac_transaction import FileStacTransaction
from .file_stac_commit import FileStacCommit


class FileStacConfig(BaseModel):
    path: str = Field(
        description="Path to repository directory"
    )


class FileStacRepository(BaseStacRepository):

    StacTransaction = FileStacTransaction
    StacCommit = FileStacCommit
    StacConfig = FileStacConfig

    _base_path: str

    def __init__(
        self,
        config: FileStacConfig
    ):
        self._base_path = os.path.abspath(config.path)

        if not os.path.isdir(os.path.abspath(config.path)):
            os.makedirs(os.path.abspath(config.path), exist_ok=True)
