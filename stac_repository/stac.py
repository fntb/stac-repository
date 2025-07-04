from __future__ import annotations

from typing import (
    Optional,
    Protocol,
    Any,
    List,
    Dict,
    Literal,
    Iterator,
    Tuple,
    Annotated
)

from collections import deque

import io
import os
import orjson
from urllib.parse import urljoin, urlparse
import datetime

import shapely

from stac_pydantic import (
    Item as _Item,
    Collection as _Collection,
    Catalog as _Catalog,
)

from stac_pydantic.collection import (
    Extent,
    SpatialExtent,
    TimeInterval
)

from stac_pydantic.shared import (
    Asset as _Asset,
    MimeTypes,
)

from stac_pydantic.links import (
    Link as _Link,
)

from pydantic import (
    BaseModel,
    ValidationError,
    ValidationInfo,
    field_validator,
    Field
)


class Link(_Link):
    _target: Optional[str | Item | Collection | Catalog] = None

    @property
    def target(self):
        return self._target

    @target.setter
    def target(self, value: str | Item | Collection | Catalog | None):
        self._target = value


class Asset(_Asset):
    _target: Optional[str] = None

    @property
    def target(self):
        return self._target

    @target.setter
    def target(self, value: str | None):
        self._target = value


class StacObject(BaseModel):
    type: Literal["Feature", "Collection", "Catalog"]


class Item(_Item):
    links: List[Link]
    assets: Dict[str, Asset]

    target: str = Field(exclude=True)

    @field_validator("target", mode="before")
    @classmethod
    def set_target(cls, value: None, info: ValidationInfo) -> str:
        return info.context


class Collection(_Collection):
    links: List[Link]
    assets: Optional[Dict[str, Asset]] = None

    target: str = Field(exclude=True)

    @field_validator("target", mode="before")
    @classmethod
    def set_target(cls, value: None, info: ValidationInfo) -> str:
        return info.context


class Catalog(_Catalog):
    links: List[Link]

    target: str = Field(exclude=True)

    @field_validator("target", mode="before")
    @classmethod
    def set_target(cls, value: None, info: ValidationInfo) -> str:
        return info.context


class ReadableStacIO(Protocol):

    def get(self, href: str) -> Any:
        ...

    def get_asset(self, href: str) -> io.RawIOBase | io.BufferedIOBase:
        ...


class StacIO(ReadableStacIO):

    def set(self, href: str, value: Any):
        ...

    def set_asset(self, href: str, value: io.RawIOBase | io.BufferedIOBase):
        ...

    def unset(self, href: str):
        ...


class DefaultStacIO():

    def get(self, href: str) -> Any:
        with open(href, "rb") as object_stream:
            return orjson.loads(object_stream.read())

    def get_asset(self, href: str) -> io.RawIOBase | io.BufferedIOBase:
        return open(href, "rb")

    def set(self, href: str, value: Any):
        with open(href, "wb") as object_stream:
            object_stream.write(orjson.dumps(value))

    def set_asset(self, href: str, value: io.RawIOBase | io.BufferedIOBase):
        with open(href, "wb") as asset_stream:
            while (chunk := value.read()):
                asset_stream.write(chunk)

    def unset(self, href: str):
        os.remove(href)


class StacObjectError(ValueError):
    ...


def is_in_scope(
    href: str,
    scope: Optional[str] = None
) -> bool:
    if scope is None:
        return True
    else:
        return href.startswith(scope)


def search(
    root_href: str,
    id: str,
    scope: Optional[str] = None,
    store: ReadableStacIO = DefaultStacIO()
) -> Item | Collection | Catalog | None:
    """Walks the catalog, without loading it all into memory at once, to find a STAC Object with some id."""

    try:
        stac_object = load(
            root_href,
            scope=scope,
            store=store
        )
    except FileNotFoundError:
        return None

    if stac_object.id == id:
        return stac_object
    elif isinstance(stac_object, (Collection, Catalog)):
        for link in stac_object.links:
            if link.rel not in ("item", "child"):
                continue

            if not is_in_scope(link.href, scope=scope):
                continue

            result = search(
                link.href,
                id,
                scope=scope,
                store=store
            )

            if result is not None:
                return result

    return None


def load(
    href: str,
    recursive: bool = False,
    scope: Optional[str] = None,
    store: ReadableStacIO = DefaultStacIO()
) -> Item | Collection | Catalog:
    json_object = store.get(href)

    try:
        typed_object = StacObject.model_validate(json_object)
    except ValidationError:
        raise StacObjectError(f"{href} is not a STAC Object")

    try:
        if typed_object.type == "Catalog":
            stac_object = Catalog.model_validate(json_object, context=href)
        elif typed_object.type == "Collection":
            stac_object = Collection.model_validate(json_object, context=href)
        else:
            stac_object = Item.model_validate(json_object, context=href)
    except ValidationError as error:
        raise StacObjectError(f"{href} is not a valid STAC Object") from error

    for link in stac_object.links:
        link.href = urljoin(href, link.href)

    if isinstance(stac_object, (Item, Collection)):
        if stac_object.assets:
            for asset in stac_object.assets.values():
                asset.href = urljoin(href, asset.href)

    if recursive:
        def resolve_child_link(link: Link) -> Link | None:
            if link.rel not in ["child", "item"] or not is_in_scope(link.href, scope=scope):
                return link

            try:
                child = load(
                    link.href,
                    recursive=recursive,
                    scope=scope,
                    store=store
                )
            except FileNotFoundError:
                return None

            link.target = child

        stac_object.links = [
            resolved_link
            for link in stac_object.links
            if (resolved_link := resolve_child_link(link)) is not None
        ]


def load_parent(
    stac_object: Item | Collection | Catalog,
    store: StacIO = DefaultStacIO(),
) -> Item | Collection | Catalog:
    parent_link: Link | None
    for link in stac_object.links:
        if link.rel == "parent":
            parent_link = link
            break
    else:
        return None

    parent = load(
        parent_link.href,
        store=store
    )

    parent_link.target = parent

    for link in parent.links:
        if link.href == stac_object.target:
            link.target = stac_object

    return parent


def save(
    stac_object: Item | Collection | Catalog,
    store: StacIO = DefaultStacIO(),
):
    """Saves a STAC object and its resolved descendants and assets (i.e. those whose link target differs from their link href) into the catalog.
    """

    store.set(stac_object.target, stac_object.model_dump())

    for asset in stac_object.assets.values():
        if asset.target is not None:
            with open(asset.target, "rb") as asset_stream:
                store.set_asset(asset.href, asset_stream)

    for link in stac_object.links:
        if link.target is not None and link.rel in ["child", "item"]:
            save(link.target, store=store)


def delete(
    stac_object: Item | Collection | Catalog,
    scope: Optional[str] = None,
    store: StacIO = DefaultStacIO(),
):

    store.unset(stac_object.target)

    for asset in stac_object.assets.values():
        if is_in_scope(asset.href, scope=scope):
            store.unset(asset.href)

    for link in stac_object.links:
        if link.target is not None and link.rel in ["child", "item"]:
            delete(link.target, store=store, scope=scope)


def fromisoformat(datetime_s: str | datetime.datetime) -> datetime.datetime:
    if isinstance(datetime_s, str):
        if not datetime_s.endswith("Z"):
            return datetime.datetime.fromisoformat(datetime_s)
        else:
            return datetime.datetime.fromisoformat(datetime_s.rstrip("Z") + "+00:00")
    elif isinstance(datetime_s, datetime.datetime):
        return datetime_s
    else:
        raise TypeError(f"{str(datetime_s)} is not a datetime string")


def get_extent(
    stac_object: Item | Collection | Catalog,
    scope: Optional[str] = None,
    store: StacIO = DefaultStacIO(),
) -> Extent | None:

    if isinstance(stac_object, Item):
        bbox: Tuple[float, float, float, float]
        datetimes: Tuple[datetime.datetime, datetime.datetime]

        if stac_object.bbox is not None:
            bbox = stac_object.bbox
        elif stac_object.geometry is not None:
            bbox = tuple(*shapely.bounds(shapely.geometry.shape(stac_object.geometry)))
        else:
            raise StacObjectError(f"Item {stac_object.id} missing geometry or bbox")

        if stac_object.properties.start_datetime is not None and stac_object.properties.end_datetime is not None:
            datetimes = (
                fromisoformat(stac_object.properties.start_datetime),
                fromisoformat(stac_object.properties.end_datetime)
            )
        elif stac_object.properties.datetime is not None:
            datetimes = (
                fromisoformat(stac_object.properties.datetime),
                fromisoformat(stac_object.properties.datetime)
            )
        else:
            raise StacObjectError(f"Item {stac_object.id} missing datetime or (start_datetime, end_datetime)")

        return Extent(
            spatial=SpatialExtent(
                bbox=[bbox]
            ),
            temporal=TimeInterval(
                interval=[datetimes]
            )
        )
    elif isinstance(stac_object, Collection):
        return stac_object.extent.model_copy()
    else:
        return compute_extent(
            stac_object,
            scope=scope,
            store=store
        )


def compute_extent(
    stac_object: Item | Collection | Catalog,
    scope: Optional[str] = None,
    store: StacIO = DefaultStacIO(),
) -> Extent | None:
    """Computes a STAC object extent. Returns None without raising on (and only on) empty catalogs.

    Raises:
        StacObjectError: If the object geospatial properties are not valid
    """

    if isinstance(stac_object, Item):
        return get_extent(stac_object, scope=scope, store=store)

    is_empty = True

    bbox = [
        180.,
        90.,
        -180.,
        -90.
    ]
    datetimes = [
        None,
        None
    ]

    bboxes = deque()
    datetimess = deque()

    for link in stac_object.links:
        if link.rel not in ("item", "child"):
            continue

        if not is_in_scope(link.href, scope=scope):
            continue

        if link.target is None:
            link.target = load(
                link.href,
                store=store
            )

        child_extent = get_extent(link.target, scope=scope, store=store)

        if child_extent is None:
            continue

        is_empty = False

        child_bbox = child_extent.spatial.bbox[0]
        child_datetimes = child_extent.temporal.interval[0]

        bboxes.append(child_bbox)
        datetimess.append(child_datetimes)

        bbox[0] = min(bbox[0], child_bbox[0])
        bbox[1] = min(bbox[1], child_bbox[1])
        bbox[2] = max(bbox[2], child_bbox[2])
        bbox[3] = max(bbox[3], child_bbox[3])

        datetimes[0] = min(datetimes[0], child_datetimes[0]) if datetimes[0] is not None else child_datetimes[0]
        datetimes[1] = max(datetimes[1], child_datetimes[1]) if datetimes[1] is not None else child_datetimes[1]

    if is_empty:
        if isinstance(stac_object, Catalog):
            return None
        else:
            raise StacObjectError(f"Collection {stac_object.id} is missing an extent")

    bboxes.appendleft(bbox)
    datetimess.appendleft(datetimes)

    return Extent(
        spatial=SpatialExtent(
            bbox=list(bboxes)
        ),
        temporal=TimeInterval(
            interval=list(datetimess)
        )
    )


class VersionNotFoundError(ValueError):
    ...


def get_version(
    stac_object: Item | Collection | Catalog,
) -> str:
    """Retrieves the version of a STAC object

    Raises:
        VersionNotFoundError: No version attribute found
    """
    if isinstance(stac_object, Item):
        version = stac_object.properties.model_extra.get("version")
    else:
        version = stac_object.model_extra.get("version")

    if not version:
        raise VersionNotFoundError("Version not found")

    return version
