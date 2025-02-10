from typing import Iterator

import pystac

from .get_stac_object_ancestors import get_stac_object_ancestors
from .get_stac_object_descendants import get_stac_object_descendants


def get_stac_object_line(stac_object: pystac.STACObject) -> Iterator[pystac.STACObject]:
    yield stac_object
    yield from get_stac_object_ancestors(stac_object)
    yield from get_stac_object_descendants(stac_object)
