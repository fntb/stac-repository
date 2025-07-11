from __future__ import annotations

from typing import (
    Optional,
    List,
    Dict,
    Literal,
    Iterator,
    Callable,
    Any,
)


import io

from stac_pydantic import (
    Item as _Item,
    Collection as _Collection,
    Catalog as _Catalog,
)

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


class Link(_Link):
    _target: Optional[Item | Collection | Catalog] = None

    @property
    def target(self) -> Item | Collection | Catalog:
        """The resolved STAC Object."""
        if not self.is_resolved():
            raise AttributeError(f"{self.rel.capitalize()} link '{self.href}' is not resolved")

        return self._target

    def resolve(self, value: Item | Collection | Catalog):
        "Resolves the Link target."
        self._target = value

    def unresolve(self):
        self._target = None

    def is_resolved(self):
        return self._target is not None


class Asset(_Asset):
    _target: Optional[Callable[[], Iterator[io.RawIOBase | io.BufferedIOBase]]] = None

    @property
    def target(self) -> Iterator[io.RawIOBase | io.BufferedIOBase]:
        """The resolved Asset file stream."""
        if not self.is_resolved():
            raise AttributeError(f"Asset '{self.href}' is not resolved")

        return self._target()

    def resolve(self, value: Callable[[], Iterator[io.RawIOBase | io.BufferedIOBase]]):
        "Resolves the Asset target."
        self._target = value

    def unresolve(self):
        self._target = None

    def is_resolved(self):
        return self._target is not None


class StacObject(BaseModel):
    type: Literal["Feature", "Collection", "Catalog"]


class Item(_Item):
    links: List[Link]
    assets: Dict[str, Asset]

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
    links: List[Link]
    assets: Optional[Dict[str, Asset]] = None

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
    links: List[Link]

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
