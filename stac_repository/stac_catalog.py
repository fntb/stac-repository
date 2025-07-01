from typing import (
    Optional,
    Iterator,
    Tuple,
    List,
    TypeAlias,
    Callable,
    NamedTuple
)

from collections import deque
import datetime
import urllib.parse
import shapely

import pystac


def is_href_file(href: str):
    return urllib.parse.urlparse(href).scheme == ""


class VersionNotFoundError(ValueError):
    ...


def get_version(
    obj: pystac.Item | pystac.Collection | pystac.Catalog,
) -> str:
    """Retrieves the version of a STAC object

    Raises:
        VersionNotFoundError: No version attribute found
    """
    if isinstance(obj, pystac.Item):
        version = obj.properties.get("version")
    else:
        version = obj.extra_fields.get("version")

    if not version:
        raise VersionNotFoundError("Version not found")

    return version


StacObjectFactory: TypeAlias = Callable[[str], pystac.Item | pystac.Collection | pystac.Catalog]


class StacObjectError(ValueError):
    ...


def make_from_file(file: str) -> pystac.Catalog | pystac.Collection | pystac.Item:
    """Makes a pystac STAC object from its href

    Raises:
        StacObjectError: Not a STAC object
    """
    for cls in (pystac.Catalog, pystac.Collection, pystac.Item):
        try:
            return cls.from_file(file)
        except pystac.STACTypeError:
            pass

    raise StacObjectError(f"{file} is not a STAC object")


class ResolvedStacObject(NamedTuple):
    href: str
    object: pystac.Item | pystac.Collection | pystac.Catalog


class WalkedStacObject(NamedTuple):
    href: str
    object: pystac.Item | pystac.Collection | pystac.Catalog
    ancestry: List[ResolvedStacObject]


def _resolve(
    obj: Optional[pystac.Item | pystac.Collection | pystac.Catalog] = None,
    href: Optional[str] = None,
    factory: StacObjectFactory = make_from_file,
) -> ResolvedStacObject:
    """Resolve a STAC object or STAC object href

    Raises:
        ValueError: Nothing to resolve
    """
    if href is None and obj is None:
        raise ValueError("Nothing to resolve")

    if href is None:
        return ResolvedStacObject(obj.self_href, obj)
    elif obj is None:
        return ResolvedStacObject(href, factory(href))
    else:
        return ResolvedStacObject(href, obj)


def walk_children(
    *,
    obj: Optional[pystac.Item | pystac.Collection | pystac.Catalog] = None,
    href: Optional[str] = None,
    factory: StacObjectFactory = make_from_file,
    domain: Optional[str] = None,
) -> Iterator[ResolvedStacObject]:
    (href, obj) = _resolve(obj=obj, href=href, factory=factory)

    for link in obj.links:
        if link.rel not in ["item", "child"]:
            continue

        child_href = urllib.parse.urljoin(href, link.href)

        if domain is not None and not child_href.startswith(domain):
            continue

        yield ResolvedStacObject(child_href, factory(child_href))


def walk_all_children(
    *,
    obj: Optional[pystac.Item | pystac.Collection | pystac.Catalog] = None,
    href: Optional[str] = None,
    factory: StacObjectFactory = make_from_file,
    domain: Optional[str] = None,
) -> Iterator[WalkedStacObject]:
    (href, obj) = _resolve(obj=obj, href=href, factory=factory)

    ancestry = ResolvedStacObject(href, obj)

    for link in obj.links:
        if link.rel in ["item", "child"]:
            child_href = urllib.parse.urljoin(href, link.href)

            if domain is not None and not child_href.startswith(domain):
                continue

            yield WalkedStacObject(child_href, child := factory(child_href), ancestry)

            if link.rel == "child":

                for grand_child in walk_all_children(obj=child, factory=factory, domain=domain, href=child_href):
                    grand_child.ancestry.append(ancestry)
                    yield grand_child


class ObjectNotFoundError(FileNotFoundError):
    ...


def get_child(
    *,
    obj: Optional[pystac.Item | pystac.Collection | pystac.Catalog] = None,
    href: Optional[str] = None,
    id: str,
    factory: StacObjectFactory = make_from_file,
    domain: Optional[str] = None,
) -> WalkedStacObject:
    """Searches the descendants of a STAC object.

    Raises:
        ObjectNotFoundError: If the object `id` cannot be found in the catalog
    """
    (href, obj) = _resolve(obj=obj, href=href, factory=factory)

    for child in walk_all_children(obj=obj, factory=factory, domain=domain, href=href):
        if child.object.id == id:
            return child

    raise ObjectNotFoundError(f"Stac object {id} not found in catalog {obj.id}")


def get_extent(
    *,
    obj: Optional[pystac.Item | pystac.Collection | pystac.Catalog] = None,
    href: Optional[str] = None,
    factory: StacObjectFactory = make_from_file,
    domain: Optional[str] = None,
) -> pystac.Extent | None:
    """Get (or compute) a STAC object extent. Returns None without raising on (and only on) empty catalogs.

    Raises:
        StacObjectError: If the object geospatial properties are not valid
    """
    (href, obj) = _resolve(obj=obj, href=href, factory=factory)

    if isinstance(obj, pystac.Item):
        bbox: Tuple[float, float, float, float]
        datetimes: Tuple[datetime.datetime, datetime.datetime]

        if obj.bbox is not None:
            bbox = obj.bbox
        elif obj.geometry is not None:
            bbox = tuple(*shapely.bounds(shapely.geometry.shape(obj.geometry)))
        else:
            raise StacObjectError(f"Item {obj.id} missing geometry or bbox")

        if obj.common_metadata.start_datetime is not None and obj.common_metadata.end_datetime is not None:
            datetimes = (obj.common_metadata.start_datetime, obj.common_metadata.end_datetime)
        elif obj.datetime is not None:
            datetimes = (obj.datetime, obj.datetime)
        else:
            raise StacObjectError(f"Item {obj.id} missing datetime or (start_datetime, end_datetime)")

        return pystac.Extent(
            pystac.SpatialExtent([bbox]),
            pystac.TemporalExtent([datetimes])
        )
    elif isinstance(obj, pystac.Collection):
        return obj.extent.clone()
    else:
        return compute_extent(obj=obj, factory=factory, domain=domain, href=href)


def compute_extent(
    *,
    obj: Optional[pystac.Item | pystac.Collection | pystac.Catalog] = None,
    href: Optional[str] = None,
    factory: StacObjectFactory = make_from_file,
    domain: Optional[str] = None,
) -> pystac.Extent | None:
    """Computes a STAC object extent. Returns None without raising on (and only on) empty catalogs.

    Raises:
        StacObjectError: If the object geospatial properties are not valid
    """

    (href, obj) = _resolve(obj=obj, href=href, factory=factory)

    if isinstance(obj, pystac.Item):
        return get_extent(obj=obj, factory=factory, domain=domain, href=href)

    is_empty = True

    bbox = (
        180.,
        90.,
        -180.,
        -90.
    )
    datetimes = (
        None,
        None
    )

    bboxes = deque()
    datetimess = deque()

    for child in walk_children(obj=obj, factory=factory, domain=domain, href=href):
        child_extent = get_extent(obj=child.object, factory=factory, domain=domain, href=child.href)

        if child_extent is None:
            continue

        is_empty = False

        child_bbox = child_extent.spatial.bboxes[0]
        child_datetimes = child_extent.temporal.intervals[0]

        bboxes.append(child_bbox)
        datetimess.append(child_datetimes)

        bbox[0] = min(bbox[0], child_bbox[0])
        bbox[1] = min(bbox[1], child_bbox[1])
        bbox[2] = max(bbox[2], child_bbox[2])
        bbox[3] = max(bbox[3], child_bbox[3])

        datetimes[0] = min(datetimes[0], child_datetimes[0]) if datetimes[0] is not None else child_datetimes[0]
        datetimes[1] = max(datetimes[1], child_datetimes[1]) if datetimes[1] is not None else child_datetimes[1]

    if is_empty:
        if isinstance(obj, pystac.Catalog):
            return None
        else:
            raise StacObjectError(f"Collection {obj.id} is missing an extent")

    bboxes.appendleft(bbox)
    datetimess.appendleft(datetimes)

    return pystac.Extent(
        pystac.SpatialExtent(list(bboxes)),
        pystac.TemporalExtent(list(datetimess))
    )
