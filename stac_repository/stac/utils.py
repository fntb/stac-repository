from __future__ import annotations

from typing import (
    Optional,
    Tuple,
    NamedTuple,
    List,
    Dict
)

from collections import deque

import logging
import os
import shutil
import datetime
import posixpath
from urllib.parse import (
    urljoin,
    urlparse as _urlparse
)

import orjson
import shapely

from stac_pydantic.shared import (
    MimeTypes,
)

from pydantic import (
    ValidationError,
)

from .models import (
    StacObject,
    Item,
    Collection,
    Catalog,
    Link,
    Asset,
    Extent,
    SpatialExtent,
    TemporalExtent
)

from .stac_io import (
    ReadableStacIO,
    StacIO,
    DefaultStacIO,
    JSONObjectError,
    FileNotInRepositoryError
)

logger = logging.getLogger(__file__)


class StacObjectError(ValueError):
    """Object does not conform to STAC specs."""
    ...


class VersionNotFoundError(ValueError):
    """STAC Object is not versioned."""
    ...


def urlrel(href: str, base_href: str) -> str:

    url = _urlparse(href, scheme="")
    base_url = _urlparse(base_href, scheme="")

    if url.scheme == "" and not posixpath.isabs(href):
        rel_href = href
    elif url[0:2] != base_url[0:2]:
        rel_href = href
    else:
        base_dir_path = "." + posixpath.dirname(base_url.path)
        path = "." + url.path

        rel_href = posixpath.relpath(path, base_dir_path)

    return rel_href


def urlpath(href: str) -> str:
    return _urlparse(href).path


def load(
    href: str,
    *,
    resolve_descendants: bool = False,
    resolve_assets: bool = False,
    store: ReadableStacIO = DefaultStacIO()
) -> Item | Collection | Catalog:
    """Loads and validates a STAC Object.

    Computes Link and Asset absolute hrefs.

    If `recursive` is True, the Object descendants (children and items) are loaded too.

    **Descendants which do not exist (i.e. FileNotFoundError) or are not valid
    STAC Objects (i.e. StacObjectError) are ignored (and removed from their parent links).**

    Raises:
        FileNotFoundError: The (root) href doesn't exist
        FileNotInRepositoryError: The (root) href is not in the repository
        StacObjectError: The retrieved (root) JSON Object is not a valid representation of a STAC Object
    """
    try:
        json_object = store.get(href)
    except JSONObjectError as error:
        raise StacObjectError(f"{href} is not a STAC Object") from error

    try:
        typed_object = StacObject.model_validate(json_object)
    except ValidationError:
        raise StacObjectError(f"{href} is not a STAC Object")

    try:
        if typed_object.type == "Feature":
            stac_object = Item.model_validate(json_object, context=href)
        elif typed_object.type == "Collection":
            stac_object = Collection.model_validate(json_object, context=href)
        elif typed_object.type == "Catalog":
            stac_object = Catalog.model_validate(json_object, context=href)
        else:
            raise StacObjectError(f"{href} doesn't have a valid STAC Object type : '{typed_object.type}'") from error
    except ValidationError as error:
        raise StacObjectError(f"{href} is not a valid STAC Object") from error

    for link in stac_object.links:
        link.href = urljoin(href, link.href)

    if isinstance(stac_object, (Item, Collection)) and stac_object.assets is not None:
        for asset in stac_object.assets.values():
            asset.href = urljoin(href, asset.href)

            if resolve_assets and asset.href.startswith(store._base_href):
                asset.resolve(lambda href=asset.href: store.get_asset(href))

    if resolve_descendants:
        resolved_links: List[Link] = []

        for link in stac_object.links:
            if not link.href.startswith(store._base_href):
                resolved_links.append(link)
            elif link.rel not in ["child", "item"]:
                resolved_links.append(link)
            else:
                try:
                    child = load(
                        link.href,
                        resolve_descendants=resolve_descendants,
                        resolve_assets=resolve_assets,
                        store=store
                    )
                except (FileNotFoundError, StacObjectError) as error:
                    logger.exception(f"[{type(error).__name__}] Ignored object {link.href} : {str(error)}")
                else:
                    link.resolve(child)

                    for child_link in child.links:
                        if child_link.rel == "parent":
                            child_link.resolve(stac_object)

                    if isinstance(child, Item) and isinstance(stac_object, Collection):
                        for child_link in child.links:
                            if child_link.rel == "collection":
                                child_link.resolve(stac_object)

                    resolved_links.append(link)

        stac_object.links = resolved_links

    return stac_object


def load_parent(
    stac_object: Item | Collection | Catalog,
    *,
    resolve_assets: bool = False,
    store: ReadableStacIO = DefaultStacIO(),
) -> Item | Collection | Catalog | None:
    """Loads and validate the parent of a STAC Object, if it has one. Resolves links.

    If the parent does not exist (i.e. FileNotFoundError) or is not valid STAC Objects (i.e. StacObjectError)
    it is ignored and the link is removed from the child.

    Raises:
        FileNotInRepositoryError: Parent is not in the repository
    """

    def resolve_child_link(parent: Item | Collection | Catalog):
        for link in parent.links:
            if link.is_resolved():
                continue

            if link.rel not in ["item", "child"]:
                continue

            if link.href == stac_object.self_href:
                link.resolve(stac_object)

    parent_link: Link

    for link in stac_object.links:
        if link.rel == "parent":
            parent_link = link
            break
    else:
        return None

    if parent_link.is_resolved():
        resolve_child_link(parent_link.target)
        return parent_link.target

    try:
        parent = load(
            parent_link.href,
            resolve_assets=resolve_assets,
            store=store,
        )
    except (FileNotFoundError, StacObjectError) as error:
        logger.exception(
            f"[{type(error).__name__}] Couldn't load parent {parent_link.href}, stripping link : {str(error)}"
        )
        stac_object.links.remove(parent_link)
    else:
        parent_link.resolve(parent)
        resolve_child_link(parent)
        return parent


def set_parent(
    stac_object: Item | Collection | Catalog,
    parent: Collection | Catalog,
):
    """Sets the parent link of a STAC Object and the parent child link. Resolves them."""

    unset_parent(stac_object)

    parent_link = Link(
        href=parent.self_href,
        rel="parent",
        type=MimeTypes.json,
    )

    parent_link.resolve(parent)

    child_link: Link
    if isinstance(stac_object, Item):
        child_link = Link(
            href=stac_object.self_href,
            rel="item",
            type=MimeTypes.json,
        )
    else:
        child_link = Link(
            href=stac_object.self_href,
            rel="child",
            type=MimeTypes.json,
        )

    child_link.resolve(stac_object)

    stac_object.links.append(parent_link)
    parent.links.append(child_link)

    if isinstance(stac_object, Item) and isinstance(parent, Collection):
        collection_link = Link(
            href=parent.self_href,
            rel="collection",
            type=MimeTypes.json
        )
        collection_link.resolve(parent)

        stac_object.links.append(collection_link)
        stac_object.collection = parent.id


def unset_parent(
    stac_object: Item | Collection | Catalog,
):
    """Removes the parent link of a STAC Object.

    If the link is resolved then the child links are removed from the parent.
    """

    links: List[Link] = []
    parent: Collection | Catalog | None = None

    for link in stac_object.links:
        if link.rel == "parent":
            if link.is_resolved():
                parent = link.target
                parent.links = [link for link in parent.links if link.href != stac_object.self_href]
        elif link.rel == "collection":
            if link.is_resolved():
                parent = link.target
                parent.links = [link for link in parent.links if link.href != stac_object.self_href]
        else:
            links.append(link)

    if isinstance(stac_object, Item) and parent is not None and isinstance(parent, Collection):
        stac_object.collection = None

    stac_object.links = links


def search(
    root_href: str | Item | Collection | Catalog,
    id: str,
    store: ReadableStacIO = DefaultStacIO()
) -> Item | Collection | Catalog | None:
    """Walks the catalog - without loading it all into memory at once - to find a STAC Object with some id."""

    try:
        stac_object = load(
            root_href,
            store=store
        )
    except (FileNotFoundError, StacObjectError, FileNotInRepositoryError) as error:
        logger.exception(f"[{type(error).__name__}] Ignored object {root_href} : {str(error)}")
        return None

    if stac_object.id == id:
        return stac_object
    elif isinstance(stac_object, (Collection, Catalog)):
        for link in stac_object.links:
            if link.rel not in ("item", "child"):
                continue

            if not link.href.startswith(store._base_href):
                continue

            found_object = search(
                link.href,
                id,
                store=store
            )

            if found_object is not None:
                return found_object

    return None


def save(
    stac_object: Item | Collection | Catalog,
    store: StacIO = DefaultStacIO(),
):
    """Normalizes and saves a STAC object and its resolved descendants and assets.
    """

    normalized_links: List[Link] = []

    for link in stac_object.links:
        if link.rel in ["self", "root", "alternate"]:
            continue

        link.href = urlrel(link.href, stac_object.self_href)

        if not link.is_resolved():
            normalized_links.append(link)
            continue

        if link.rel in ["child", "item"]:
            child = link.target

            if isinstance(child, Item):
                link.href = posixpath.join(".", child.id, f"{child.id}.json")
            elif isinstance(child, Collection):
                link.href = posixpath.join(".", child.id, "collection.json")
            elif isinstance(child, Catalog):
                link.href = posixpath.join(".", child.id, "catalog.json")
            else:
                raise TypeError(f"Unexpected child type : {type(child).__name__}")

            child.self_href = urljoin(stac_object.self_href, link.href)

        elif link.rel == "parent":
            parent = link.target

            if isinstance(parent, Collection):
                link.href = posixpath.join("..", "collection.json")
            elif isinstance(parent, Catalog):
                link.href = posixpath.join("..", "catalog.json")
            else:
                raise TypeError(f"Unexpected parent type : {type(parent).__name__}")

        elif link.rel == "collection":
            parent = link.target

            if isinstance(parent, Collection):
                link.href = posixpath.join("..", "collection.json")
            else:
                raise TypeError(f"Unexpected collection type : {type(parent).__name__}")

        normalized_links.append(link)

    stac_object.links = normalized_links

    for link in stac_object.links:
        if link.is_resolved():
            if link.rel in ["child", "item"]:
                save(link.target, store=store)

    if isinstance(stac_object, (Item, Collection)) and stac_object.assets is not None:
        for asset in stac_object.assets.values():
            if asset.is_resolved():
                asset.href = posixpath.join(".", posixpath.basename(urlpath(asset.href)))
            else:
                asset.href = urlrel(asset.href, stac_object.self_href)

        saved_assets: Dict[str, Asset] = {}

        for (key, asset) in stac_object.assets.items():
            if asset.is_resolved():
                try:
                    with asset.target as asset_stream:
                        store.set_asset(urljoin(stac_object.self_href, asset.href), asset_stream)

                    asset.resolve(lambda href=urljoin(stac_object.self_href, asset.href): store.get_asset(href))
                except FileNotFoundError as error:
                    logger.exception(
                        f"[{type(error).__name__}] Ignored asset {urljoin(stac_object.self_href, asset.href)} : {str(error)}"
                    )
                else:
                    saved_assets[key] = asset

        stac_object.assets = saved_assets

    store.set(stac_object.self_href, stac_object.model_dump())


def export(
    href: str,
    file: str,
    store: StacIO = DefaultStacIO(),
):
    """Exports a STAC Object and all its descendants and assets.

    Raises:
        FileExistsError
    """

    file = os.path.abspath(file)
    dir = os.path.dirname(file)
    os.makedirs(dir, exist_ok=True)

    if os.listdir(dir):
        raise FileExistsError(f"{dir} is not empty")

    try:
        stac_object = load(href, resolve_assets=True, store=store)

        unset_parent(stac_object)

        saved_links: List[Link] = []

        for link in stac_object.links:
            if not link.href.startswith(store._base_href) or link.rel not in ["child", "item"]:
                saved_links.append(link)
                continue

            try:
                export(
                    link.href,
                    os.path.join(dir, urlrel(link.href, href)),
                    store=store
                )
            except (FileNotFoundError, StacObjectError) as error:
                logger.exception(f"[{type(error).__name__}] Ignored object {link.href} : {str(error)}")
            else:
                saved_links.append(link)

        stac_object.links = saved_links

        saved_assets: Dict[str, Asset] = {}

        if isinstance(stac_object, (Item, Collection)) and stac_object.assets is not None:
            for (key, asset) in stac_object.assets.items():
                if not asset.href.startswith(store._base_href):
                    saved_assets[key] = asset
                    continue

                asset_file = os.path.join(dir, posixpath.basename(urlpath(asset.href)))

                try:
                    with store.get_asset(asset.href) as asset_read_stream:
                        with open(asset_file, "w+b") as asset_write_stream:
                            while (chunk := asset_read_stream.read()):
                                asset_write_stream.write(chunk)
                except (FileNotFoundError) as error:
                    logger.exception(f"[{type(error).__name__}] Ignored asset {asset.href} : {str(error)}")
                else:
                    saved_assets[key] = asset

            stac_object.assets = saved_assets

        with open(file, "w+b") as object_stream:
            try:
                object_stream.write(orjson.dumps(stac_object.model_dump()))
            except orjson.JSONEncodeError as error:
                raise JSONObjectError from error

    except Exception as error:
        shutil.rmtree(dir, ignore_errors=True)
        raise error


def delete(
    href_or_stac_object: str | Item | Collection | Catalog,
    store: StacIO = DefaultStacIO(),
):
    """Deletes a STAC object and all its descendants and assets.
    """

    try:
        if isinstance(href_or_stac_object, str):
            href = href_or_stac_object
            stac_object = load(
                href,
                store=store,
            )
        else:
            href = href_or_stac_object.self_href
            stac_object = href_or_stac_object
    except FileNotInRepositoryError:
        pass
    except (FileNotFoundError, StacObjectError) as error:
        logger.exception(
            f"[{type(error).__name__}] Couldn't load object {href} - removing it will potentially create unreachable orphans : {str(error)}"
        )
        store.unset(href)
    else:
        if isinstance(stac_object, (Item, Collection)) and stac_object.assets is not None:
            for asset in stac_object.assets.values():
                store.unset(asset.href)

        if isinstance(stac_object, (Collection, Catalog)):
            for link in stac_object.links:
                if link.rel in ["child", "item"]:
                    delete(link.href, store=store)

        store.unset(href)


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


def toisoformat(datetime_t: str | datetime.datetime) -> str:
    if isinstance(datetime_t, str):
        return datetime_t
    elif isinstance(datetime_t, datetime.datetime):
        return datetime_t.isoformat()
    else:
        raise TypeError(f"{str(datetime_t)} is not a datetime")


def get_extent(
    stac_object: Item | Collection | Catalog,
    store: StacIO = DefaultStacIO(),
) -> Extent | None:
    """Retreives (or compute, if necessary) a STAC object extent. Returns None without raising on (and only on) empty catalogs.

    Raises:
        StacObjectError: If the object geospatial properties are not valid
    """

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
            temporal=TemporalExtent(
                interval=[(toisoformat(datetimes[0]), toisoformat(datetimes[1]))]
            )
        )
    elif isinstance(stac_object, Collection):
        return stac_object.extent.model_copy()
    else:
        return compute_extent(
            stac_object,
            store=store
        )


def compute_extent(
    stac_object: Item | Collection | Catalog,
    store: StacIO = DefaultStacIO(),
) -> Extent | None:
    """Computes a STAC object extent. Returns None without raising on (and only on) empty catalogs.

    Raises:
        StacObjectError: If the object geospatial properties are not valid
    """

    if isinstance(stac_object, Item):
        return get_extent(stac_object, store=store)

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

        if not link.is_resolved():
            try:
                child = load(
                    link.href,
                    store=store,
                )
            except (FileNotFoundError, StacObjectError, FileNotInRepositoryError) as error:
                logger.exception(
                    f"[{type(error).__name__}] Ignored child {link.href} while computing extent : {str(error)}")
                continue
        else:
            child = link.target

        child_extent = get_extent(child, store=store)

        if child_extent is None:
            continue

        is_empty = False

        child_bbox = child_extent.spatial.bbox[0]
        child_datetimes = (
            fromisoformat(child_extent.temporal.interval[0][0]),
            fromisoformat(child_extent.temporal.interval[0][1])
        )

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
        temporal=TemporalExtent(
            interval=[
                (toisoformat(datetimes[0]), toisoformat(datetimes[1])) for datetimes in datetimess
            ]
        )
    )


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
