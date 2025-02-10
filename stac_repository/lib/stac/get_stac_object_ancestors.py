from typing import Iterator

import pystac


def get_stac_object_ancestors(stac_object: pystac.STACObject) -> Iterator[pystac.STACObject]:
    while True:
        stac_object = stac_object.get_parent()
        if stac_object is not None:
            yield stac_object
        else:
            break
