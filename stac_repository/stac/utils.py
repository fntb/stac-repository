from __future__ import annotations

from typing import (
    Optional,
    Tuple,
    Union,
    List,
    Dict,
    overload
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
    JSONObjectError,
    HrefError
)

logger = logging.getLogger(__file__)


class StacObjectError(ValueError):
    """Object does not conform to STAC specs."""
    pass


class VersionNotFoundError(ValueError):
    """STAC object is not versioned."""
    pass


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
    io: ReadableStacIO,
) -> Union[Item, Collection, Catalog]:
    """Loads and validates a STAC object.

    Computes Link and Asset absolute hrefs.

    If `recursive` is True, the Object descendants (children and items) are loaded too.

    **Descendants which do not exist (i.e. FileNotFoundError) or are not valid
    STAC Objects (i.e. StacObjectError) are ignored (and removed from their parent links).**

    Raises:
        FileNotFoundError: The (root) href doesn't exist
        StacObjectError: The retrieved (root) JSON object is not a valid representation of a STAC object
        HrefError: The (root) href cannot be processed by this StacIO instance
    """
    try:
        json_object = io.get(href)
    except JSONObjectError as error:
        raise StacObjectError(f"{href} is not a JSON object : {str(error)}") from error

    try:
        typed_object = StacObject.model_validate(json_object)
    except ValidationError:
        raise StacObjectError(f"{href} is not a STAC object : missing 'type' property")

    try:
        if typed_object.type == "Feature":
            stac_object = Item.model_validate(json_object, context=href)
        elif typed_object.type == "Collection":
            stac_object = Collection.model_validate(json_object, context=href)
        elif typed_object.type == "Catalog":
            stac_object = Catalog.model_validate(json_object, context=href)
        else:
            raise StacObjectError(f"{href} doesn't have a valid STAC object type : '{typed_object.type}'")
    except ValidationError as error:
        raise StacObjectError(f"{href} is not a valid STAC object : {str(error)}") from error

    for link in stac_object.links:
        link.href = urljoin(href, link.href)

    if isinstance(stac_object, (Item, Collection)) and stac_object.assets is not None:
        for asset in stac_object.assets.values():
            asset.href = urljoin(href, asset.href)

            if resolve_assets:
                asset.target = lambda href=asset.href: io.get_asset(href)

    if resolve_descendants:
        resolved_links: List[Link] = []

        for link in stac_object.links:
            if link.rel not in ["child", "item"]:
                resolved_links.append(link)
            else:
                try:
                    child = load(
                        link.href,
                        resolve_descendants=resolve_descendants,
                        resolve_assets=resolve_assets,
                        io=io,
                    )
                except HrefError as error:
                    logger.exception(
                        f"[{type(error).__name__}] Ignored child {link.href} link resolution : {str(error)}"
                    )
                    resolved_links.append(link)
                except (FileNotFoundError, StacObjectError) as error:
                    logger.exception(
                        f"[{type(error).__name__}] Stipped child {link.href} from parent links : {str(error)}"
                    )
                else:
                    link.target = child

                    for child_link in child.links:
                        if child_link.rel == "parent":
                            child_link.target = stac_object

                    if isinstance(child, Item) and isinstance(stac_object, Collection):
                        for child_link in child.links:
                            if child_link.rel == "collection":
                                child_link.target = stac_object

                    resolved_links.append(link)

        stac_object.links = resolved_links

    return stac_object


def load_parent(
    stac_object: Union[Item, Collection, Catalog],
    *,
    resolve_assets: bool = False,
    io: ReadableStacIO,
) -> Optional[Union[Item, Collection, Catalog]]:
    """Loads and validate the parent of a STAC object, if it has one. Resolves links.

    If the parent does not exist (i.e. FileNotFoundError) or is not valid STAC Objects (i.e. StacObjectError)
    it is ignored and the link is removed from the child.

    Raises:
        HrefError: Parent cannot be retrieved
    """

    def resolve_child_link(parent: Union[Item, Collection, Catalog]):
        for link in parent.links:
            if link.target is not None:
                continue

            if link.rel not in ["item", "child"]:
                continue

            if link.href == stac_object.self_href:
                link.target = stac_object

    parent_link: Link

    for link in stac_object.links:
        if link.rel == "parent":
            parent_link = link
            break
    else:
        return None

    if parent_link.target is not None:
        resolve_child_link(parent_link.target)
        return parent_link.target

    try:
        parent = load(
            parent_link.href,
            resolve_assets=resolve_assets,
            io=io,
        )
    except (FileNotFoundError, StacObjectError) as error:
        logger.exception(
            f"[{type(error).__name__}] Couldn't load parent {parent_link.href}, stripping link : {str(error)}"
        )
        stac_object.links.remove(parent_link)
    else:
        parent_link.target = parent
        resolve_child_link(parent)
        return parent


def set_parent(
    stac_object: Union[Item, Collection, Catalog],
    parent: Union[Collection, Catalog],
):
    """Sets the parent link of a STAC object and the parent child link. Resolves them."""

    unset_parent(stac_object)

    parent_link = Link(
        href=parent.self_href,
        rel="parent",
        type=MimeTypes.json,
    )

    parent_link.target = parent

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

    child_link.target = stac_object

    stac_object.links.append(parent_link)
    parent.links.append(child_link)

    if isinstance(stac_object, Item) and isinstance(parent, Collection):
        collection_link = Link(
            href=parent.self_href,
            rel="collection",
            type=MimeTypes.json
        )
        collection_link.target = parent

        stac_object.links.append(collection_link)
        stac_object.collection = parent.id


def unset_parent(
    stac_object: Union[Item, Collection, Catalog],
):
    """Removes the parent link of a STAC object.

    If the link is resolved then the child links are removed from the parent.
    """

    links: List[Link] = []
    parent: Optional[Union[Item, Collection, Catalog]] = None

    for link in stac_object.links:
        if link.rel == "parent":
            if link.target is not None:
                parent = link.target
                parent.links = [link for link in parent.links if link.href != stac_object.self_href]
        elif link.rel == "collection":
            if link.target is not None:
                parent = link.target
                parent.links = [link for link in parent.links if link.href != stac_object.self_href]
        else:
            links.append(link)

    if isinstance(stac_object, Item) and parent is not None and isinstance(parent, Collection):
        stac_object.collection = None

    stac_object.links = links


def search(
    root_href: Union[str, Item, Collection, Catalog],
    id: str,
    *,
    io: ReadableStacIO,
) -> Optional[Union[Item, Collection, Catalog]]:
    """Walks the catalog - without loading it all into memory at once - to find a STAC object with some id."""

    if isinstance(root_href, str):
        try:
            stac_object = load(
                root_href,
                io=io,
            )
        except (FileNotFoundError, StacObjectError, HrefError) as error:
            logger.exception(f"[{type(error).__name__}] Ignored {root_href} : {str(error)}")
            return None
    else:
        stac_object = root_href

    if stac_object.id == id:
        return stac_object
    elif isinstance(stac_object, (Collection, Catalog)):
        for link in stac_object.links:
            if link.rel not in ("item", "child"):
                continue

            found_object = search(
                link.href,
                id,
                io=io,
            )

            if found_object is not None:
                return found_object

    return None


def save(
    stac_object: Union[Item, Collection, Catalog],
    *,
    io: StacIO,
):
    """Normalizes and saves a STAC object and its resolved descendants and assets.

    Raises:
        HrefError: Stac object could not be saved to its self_href
    """

    saved_links: List[Link] = []

    for link in stac_object.links:
        if link.rel in ["self", "root", "alternate"]:
            continue

        link.href = urlrel(link.href, stac_object.self_href)

        if link.target is None:
            saved_links.append(link)
        elif link.rel in ["child", "item"]:
            child = link.target
            saved_child_href: str

            if isinstance(child, Item):
                saved_child_href = posixpath.join(".", child.id, f"{child.id}.json")
            elif isinstance(child, Collection):
                saved_child_href = posixpath.join(".", child.id, "collection.json")
            elif isinstance(child, Catalog):
                saved_child_href = posixpath.join(".", child.id, "catalog.json")
            else:
                raise TypeError(f"Unexpected child type : {type(child).__name__}")

            child.self_href = urljoin(stac_object.self_href, saved_child_href)

            try:
                save(child, io=io)
            except HrefError as error:
                pass

            link.href = saved_child_href

            saved_links.append(link)

        elif link.rel == "parent":
            parent = link.target

            if isinstance(parent, Collection):
                link.href = posixpath.join("..", "collection.json")
            elif isinstance(parent, Catalog):
                link.href = posixpath.join("..", "catalog.json")
            else:
                raise TypeError(f"Unexpected parent type : {type(parent).__name__}")

            saved_links.append(link)

        elif link.rel == "collection":
            parent = link.target

            if isinstance(parent, Collection):
                link.href = posixpath.join("..", "collection.json")
            else:
                raise TypeError(f"Unexpected collection type : {type(parent).__name__}")

            saved_links.append(link)

    stac_object.links = saved_links

    if isinstance(stac_object, (Item, Collection)) and stac_object.assets is not None:
        saved_assets: Dict[str, Asset] = {}

        for (key, asset) in stac_object.assets.items():
            if asset.target is not None:
                saved_asset_href = posixpath.join(".", "assets", posixpath.basename(urlpath(asset.href)))

                try:
                    with asset.target() as asset_stream:
                        io.set_asset(urljoin(stac_object.self_href, saved_asset_href), asset_stream)
                except FileNotFoundError as error:
                    logger.exception(
                        f"[{type(error).__name__}] Ignored asset {urljoin(stac_object.self_href, asset.href)} : {str(error)}"
                    )
                except HrefError as error:
                    saved_assets[key] = asset
                else:
                    asset.href = saved_asset_href
                    asset.target = lambda href=urljoin(stac_object.self_href, asset.href): io.get_asset(href)

                    saved_assets[key] = asset
            else:
                asset.href = urlrel(asset.href, stac_object.self_href)
                saved_assets[key] = asset

        stac_object.assets = saved_assets

    io.set(stac_object.self_href, stac_object.model_dump())


def export(
    href: str,
    file: str,
    *,
    io: StacIO,
):
    """Exports a STAC object and all its descendants and assets.

    Raises:
        FileExistsError
    """
    raise NotImplementedError

    file = os.path.abspath(file)
    dir = os.path.dirname(file)
    os.makedirs(dir, exist_ok=True)

    if os.listdir(dir):
        raise FileExistsError(f"{dir} is not empty")

    try:
        stac_object = load(href, resolve_assets=True, io=io)

        unset_parent(stac_object)

        saved_links: List[Link] = []

        for link in stac_object.links:
            if not link.href.startswith(io._base_href) or link.rel not in ["child", "item"]:
                saved_links.append(link)
                continue

            try:
                export(
                    link.href,
                    os.path.join(dir, urlrel(link.href, href)),
                    io=io
                )
            except (FileNotFoundError, StacObjectError) as error:
                logger.exception(f"[{type(error).__name__}] Ignored object {link.href} : {str(error)}")
            else:
                saved_links.append(link)

        stac_object.links = saved_links

        saved_assets: Dict[str, Asset] = {}

        if isinstance(stac_object, (Item, Collection)) and stac_object.assets is not None:
            for (key, asset) in stac_object.assets.items():
                if not asset.href.startswith(io._base_href):
                    saved_assets[key] = asset
                    continue

                asset_file = os.path.join(dir, posixpath.basename(urlpath(asset.href)))

                try:
                    with io.get_asset(asset.href) as asset_read_stream:
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
    href_or_stac_object: Union[str, Item, Collection, Catalog],
    *,
    io: StacIO,
):
    """Deletes a STAC object and all its descendants and assets as best as it can.
    """
    href: str
    stac_object: Optional[Union[Item, Collection, Catalog]] = None

    def silent_unset(href: str):
        try:
            io.unset(href)
        except HrefError as error:
            pass

    def delete_assets(stac_object: Union[Item, Collection, Catalog]):
        if isinstance(stac_object, (Item, Collection)) and stac_object.assets is not None:
            for asset in stac_object.assets.values():
                silent_unset(asset.href)

    def delete_children(stac_object: Union[Item, Collection, Catalog]):
        if isinstance(stac_object, (Collection, Catalog)):
            for link in stac_object.links:
                if link.rel in ["child", "item"]:
                    delete(link.href, io=io)

    if isinstance(href_or_stac_object, (Item, Collection, Catalog)):
        stac_object = href_or_stac_object

        delete_assets(stac_object)
        delete_children(stac_object)
        silent_unset(stac_object.self_href)

    elif isinstance(href_or_stac_object, str):
        href = href_or_stac_object

        try:
            stac_object = load(
                href,
                io=io,
            )
        except FileNotFoundError as error:
            return
        except HrefError as error:
            return
        except StacObjectError as error:
            logger.exception(
                f"[{type(error).__name__}] {href} is not a valid Stac object - removing it may create unreachable orphans : {str(error)}"
            )
            silent_unset(href)
        else:
            delete_assets(stac_object)
            delete_children(stac_object)
            silent_unset(href)
    else:
        raise TypeError(f"{type(href_or_stac_object)} is neither a Stac object or uri")


@overload
def fromisoformat(datetime_s: Union[str, datetime.datetime]) -> datetime.datetime:
    ...


@overload
def fromisoformat(datetime_s: None) -> None:
    ...


def fromisoformat(datetime_s: Optional[Union[str, datetime.datetime]]) -> Optional[datetime.datetime]:
    if datetime_s is None:
        return None
    elif isinstance(datetime_s, str):
        if not datetime_s.endswith("Z"):
            return datetime.datetime.fromisoformat(datetime_s)
        else:
            return datetime.datetime.fromisoformat(datetime_s.rstrip("Z") + "+00:00")
    elif isinstance(datetime_s, datetime.datetime):
        return datetime_s
    else:
        raise TypeError(f"{str(datetime_s)} is not a datetime string")


@overload
def toisoformat(datetime_t: Union[str, datetime.datetime]) -> str:
    ...


@overload
def toisoformat(datetime_t: None) -> None:
    ...


def toisoformat(datetime_t: Optional[Union[str, datetime.datetime]]) -> Optional[str]:
    if datetime_t is None:
        return None
    elif isinstance(datetime_t, str):
        return datetime_t
    elif isinstance(datetime_t, datetime.datetime):
        return datetime_t.isoformat()
    else:
        raise TypeError(f"{str(datetime_t)} is not a datetime")


@overload
def get_extent(
    stac_object: Union[Item, Collection],
    *,
    io: StacIO,
) -> Extent:
    ...


@overload
def get_extent(
    stac_object: Catalog,
    *,
    io: StacIO,
) -> Optional[Extent]:
    ...


def get_extent(
    stac_object: Union[Item, Collection, Catalog],
    *,
    io: StacIO,
) -> Optional[Extent]:
    """Retreives (or compute, if necessary) a STAC object extent. Returns None without raising on (and only on) empty catalogs.

    Raises:
        StacObjectError: If the object geospatial properties are not valid
    """

    if isinstance(stac_object, Item):
        bbox: Tuple[float, float, float, float]
        datetimes: Tuple[datetime.datetime, datetime.datetime]

        if stac_object.bbox is not None:
            if len(stac_object.bbox) != 4:
                raise NotImplementedError(f"3-dimensional bbox encountered on item {stac_object.id}")

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
                bbox=[list(bbox)]
            ),
            temporal=TemporalExtent(
                interval=[[toisoformat(datetimes[0]), toisoformat(datetimes[1])]]
            )
        )
    elif isinstance(stac_object, Collection):
        return stac_object.extent.model_copy()
    else:
        return compute_extent(
            stac_object,
            io=io
        )


@overload
def compute_extent(
    stac_object: Union[Item, Collection],
    *,
    io: StacIO,
) -> Extent:
    ...


@overload
def compute_extent(
    stac_object: Catalog,
    *,
    io: StacIO,
) -> Optional[Extent]:
    ...


def compute_extent(
    stac_object: Union[Item, Collection, Catalog],
    *,
    io: StacIO,
) -> Optional[Extent]:
    """Computes a STAC object extent. Returns None without raising on (and only on) empty catalogs.

    Raises:
        StacObjectError: If the object geospatial properties are not valid
    """

    if isinstance(stac_object, Item):
        return get_extent(stac_object, io=io)

    is_empty = True

    bbox: List[float] = [
        180.,
        90.,
        -180.,
        -90.
    ]
    datetimes: List[Optional[datetime.datetime]] = [
        None,
        None
    ]

    bboxes = deque()
    datetimess = deque()

    for link in stac_object.links:
        if link.rel not in ("item", "child"):
            continue

        if link.target is None:
            try:
                child = load(
                    link.href,
                    io=io,
                )
            except (FileNotFoundError, StacObjectError, HrefError) as error:
                logger.exception(
                    f"[{type(error).__name__}] Ignored child {link.href} while computing extent : {str(error)}")
                continue
        else:
            child = link.target

        child_extent = get_extent(child, io=io)

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

        if datetimes[0] is None or child_datetimes[0] is None:
            datetimes[0] = None
        else:
            datetimes[0] = min(datetimes[0], child_datetimes[0])

        if datetimes[1] is None or child_datetimes[1] is None:
            datetimes[1] = None
        else:
            datetimes[1] = max(datetimes[1], child_datetimes[1])

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
                [toisoformat(datetimes[0]), toisoformat(datetimes[1])] for datetimes in datetimess
            ]
        )
    )


def get_version(
    stac_object: Union[Item, Collection, Catalog],
) -> str:
    """Retrieves the version of a STAC object

    Raises:
        VersionNotFoundError: No version attribute found
        StacObjectError: Version is not a string
    """
    if isinstance(stac_object, Item) and stac_object.properties.model_extra is not None:
        version = stac_object.properties.model_extra.get("version")
    elif isinstance(stac_object, (Collection, Catalog)) and stac_object.model_extra is not None:
        version = stac_object.model_extra.get("version")
    else:
        raise TypeError(f"{type(stac_object).__name__} is not a stac object.")

    if version is None:
        raise VersionNotFoundError("Version not found")
    elif not isinstance(version, str):
        raise StacObjectError(f"Stac Object {stac_object.id} \"version\" property is not a string")

    return version


def set_version(
    stac_object: Union[Item, Collection, Catalog],
    version: str
) -> str:
    """Sets the version of a STAC object.
    """
    if not isinstance(version, str):
        raise TypeError(f"{version} is not a string")

    if isinstance(stac_object, Item):
        if stac_object.properties.model_extra is not None:
            stac_object.properties.model_extra["version"] = version
        else:
            stac_object.properties.model_extra = {"version": version}
    elif isinstance(stac_object, (Collection, Catalog)) and stac_object.model_extra is not None:
        if stac_object.model_extra is not None:
            stac_object.model_extra["version"] = version
        else:
            stac_object.model_extra = {"version": version}
    else:
        raise TypeError(f"{type(stac_object).__name__} is not a stac object.")
