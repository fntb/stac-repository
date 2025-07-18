from typing import (
    Type,
)
import sys

if sys.version_info >= (3, 9):
    from typing import Protocol
else:
    from typing_extensions import Protocol

from .base_stac_repository import BaseStacRepository


class Backend(Protocol):

    __version__: str

    StacRepository: Type[BaseStacRepository]
