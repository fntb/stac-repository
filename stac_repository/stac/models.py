from __future__ import annotations

from typing import (
    Optional,
    List,
    Dict,
    Literal,
    Iterator,
    Callable,
    Union,
    Any,
    BinaryIO,
)

from contextlib import contextmanager, AbstractContextManager

import sys

from stac_pydantic.item import Item as _Item
from stac_pydantic.collection import Collection as _Collection
from stac_pydantic.catalog import Catalog as _Catalog

from stac_pydantic.collection import (
    Extent,
    SpatialExtent,
    TimeInterval as TemporalExtent
)

from stac_pydantic.shared import (
    Asset as _Asset,
)

from stac_pydantic.links import (
    Link as _Link,
)

from pydantic import (
    BaseModel,
    field_validator,
    field_serializer,
    model_validator,
    Field,
    ValidationInfo
)

from pydantic import (
    AnyUrl
)

if sys.version_info >= (3, 10):
    from typing import TypeGuard
else:
    from typing_extensions import TypeGuard


class Link(_Link):
    _target: Optional[Union[Item, Collection, Catalog]] = None

    @property
    def target(self) -> Optional[Union[Item, Collection, Catalog]]:
        """The resolved STAC Object."""
        # if self._target is None:
        #     raise AttributeError(f"{self.rel.capitalize()} link '{self.href}' is not resolved")

        return self._target

    @target.setter
    def target(self, value: Union[Item, Collection, Catalog, None]):
        self._target = value


class Asset(_Asset):
    _target: Optional[Callable[[], AbstractContextManager[BinaryIO]]] = None

    @property
    @contextmanager
    def target(self) -> Iterator[Optional[BinaryIO]]:
        """The resolved Asset file stream."""
        # if self._target is None:
        #     raise AttributeError(f"Asset '{self.href}' is not resolved")

        if self._target is None:
            yield None
        else:
            with self._target() as target:
                yield target

    @target.setter
    def target(self, value: Optional[Callable[[], AbstractContextManager[BinaryIO]]]):
        self._target = value


class StacObject(BaseModel):
    type: Literal["Feature", "Collection", "Catalog"]


class Item(_Item):
    links: List[Link]  # type: ignore
    assets: Dict[str, Asset]  # type: ignore

    self_href: str = Field(exclude=True)

    @model_validator(mode="before")
    @classmethod
    def add_self_href(cls, data: Dict[str, Any], info: ValidationInfo):
        data["self_href"] = info.context
        return data

    @field_serializer("stac_extensions")
    def serialize_stac_extensions(self, value: Optional[List[AnyUrl]], _info):
        if value is None:
            return None

        return [
            str(url) for url in value
        ]


class Collection(_Collection):
    links: List[Link]  # type: ignore
    assets: Optional[Dict[str, Asset]] = None  # type: ignore

    self_href: str = Field(exclude=True)

    @model_validator(mode="before")
    @classmethod
    def add_self_href(cls, data: Dict[str, Any], info: ValidationInfo):
        data["self_href"] = info.context
        return data

    @field_serializer("stac_extensions")
    def serialize_stac_extensions(self, value: Optional[List[AnyUrl]], _info):
        if value is None:
            return None

        return [
            str(url) for url in value
        ]


class Catalog(_Catalog):
    links: List[Link]  # type: ignore

    self_href: str = Field(exclude=True)

    @model_validator(mode="before")
    @classmethod
    def add_self_href(cls, data: Dict[str, Any], info: ValidationInfo):
        data["self_href"] = info.context
        return data

    @field_serializer("stac_extensions")
    def serialize_stac_extensions(self, value: Optional[List[AnyUrl]], _info):
        if value is None:
            return None

        return [
            str(url) for url in value
        ]
