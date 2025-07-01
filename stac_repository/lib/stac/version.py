
import pystac


def version(stac_object: pystac.STACObject) -> str:
    return str(hash(stac_object))
