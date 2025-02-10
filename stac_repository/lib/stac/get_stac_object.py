
import pystac

from .walk_stac_object import walk_stac_object


def get_stac_object(catalog: pystac.Catalog, id: str, recursive: True = True) -> pystac.STACObject | None:
    for stac_object in walk_stac_object(catalog):
        if stac_object.id == id:
            return stac_object

    return None
