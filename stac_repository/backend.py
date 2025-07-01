from typing import (
    Protocol,
    Type,
    Optional
)

from .base_stac_repository import BaseStacRepository
from .base_stac_commit import BaseStacCommit
from .base_stac_transaction import BaseStacTransaction

import pydantic


class Backend(Protocol):

    __version__: str

    Config: Optional[Type[pydantic.BaseModel]]

    StacRepository: Type[BaseStacRepository]
    StacCommit: Type[BaseStacCommit]
    StacTransaction: Type[BaseStacTransaction]
