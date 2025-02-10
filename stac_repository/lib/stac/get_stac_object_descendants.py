from typing import Iterator

import pystac


def get_stac_object_descendants(stac_object: pystac.STACObject) -> Iterator[pystac.STACObject]:

    if isinstance(stac_object, (pystac.Catalog, pystac.Collection)):
        yield from stac_object.get_items()

        for child in stac_object.get_children():
            yield child
            yield from get_stac_object_descendants(child)
