from typing import Iterator

import pystac


def get_stac_object_hrefs(stac_object: pystac.STACObject) -> Iterator[str]:
    if stac_object.get_self_href() is not None:
        yield stac_object.get_self_href()

    for link in stac_object.links:
        if link.get_absolute_href() is not None:
            yield link.get_absolute_href()

    if isinstance(stac_object, (pystac.Item, pystac.Collection)):
        for asset in stac_object.assets.values():
            if asset.get_absolute_href() is not None:
                yield asset.get_absolute_href()
