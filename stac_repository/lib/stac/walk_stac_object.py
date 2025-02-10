from typing import Iterator

import pystac


def walk_stac_object(stac_object: pystac.STACObject, cls: pystac.STACObject | tuple[pystac.STACObject] | None = None) -> Iterator[pystac.STACObject]:

    if cls is None or isinstance(stac_object, cls):
        yield stac_object

    if isinstance(stac_object, (pystac.Collection, pystac.Catalog)):
        if cls is None or cls is pystac.Item:
            for item in stac_object.get_items():
                yield item

        for child in stac_object.get_children():
            yield from walk_stac_object(child)
