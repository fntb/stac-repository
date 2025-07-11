from typing import (
    Protocol,
    Type,
)

from .base_stac_repository import BaseStacRepository


class Backend(Protocol):

    __version__: str

    StacRepository: Type[BaseStacRepository]
