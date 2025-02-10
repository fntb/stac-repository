from os import PathLike
from os import path
from typing import Iterator
import tempfile
import shutil

import pystac
import pystac.layout

from .processor import ProductFactory
from .processor import SimpleProduct

__version__ = "0.0.1"


def discover(source: str) -> Iterator[str]:
    yield from ProductFactory.discover(source)


def id(product_source: str) -> str:
    product = ProductFactory.make(product_source)
    return product.id


def version(product_source: str) -> str:
    product = ProductFactory.make(product_source)
    return product.version


def process(product_source: str) -> PathLike[str]:
    product = ProductFactory.make(product_source)
    dir = tempfile.mkdtemp()

    try:
        stac_object = product.to_stac_object()

        if isinstance(stac_object, pystac.Item):
            file = path.join(dir, "item.json")
            stac_object.save_object(
                dest_href=file
            )
        elif isinstance(stac_object, pystac.Collection):
            file = path.join(dir, "collection.json")
            stac_object.normalize_and_save(
                file,
                catalog_type=pystac.CatalogType.SELF_CONTAINED,
                strategy=pystac.layout.BestPracticesLayoutStrategy(),
            )
        elif isinstance(stac_object, pystac.Catalog):
            file = path.join(dir, "catalog.json")
            stac_object.normalize_and_save(
                file,
                catalog_type=pystac.CatalogType.SELF_CONTAINED,
                strategy=pystac.layout.BestPracticesLayoutStrategy(),
            )

    except Exception as error:
        shutil.rmtree(dir, ignore_errors=True)
        raise error

    return file


def catalog(processed_product_stac_object_file: PathLike[str], *, catalog_file: PathLike[str]) -> None:
    catalog = pystac.Catalog.from_file(catalog_file)

    demo_catalog = catalog.get_child("demo")
    if demo_catalog is None:
        demo_catalog = pystac.Catalog(
            "demo",
            "# Demo Processor Catalog",
            "Demo Catalog",
            catalog_type=pystac.CatalogType.SELF_CONTAINED,
            strategy=pystac.layout.BestPracticesLayoutStrategy()
        )

        catalog.add_child(demo_catalog)
        catalog.save()

    errors = []
    for StacObject in (pystac.Item, pystac.Collection, pystac.Catalog):
        try:
            stac_object = StacObject.from_file(
                processed_product_stac_object_file)
            break
        except Exception as error:
            errors.append(error)
    else:
        raise ExceptionGroup("Couldn't parse processed product", errors)

    if isinstance(stac_object, pystac.Item):
        demo_catalog.add_item(stac_object)
    elif isinstance(stac_object, (pystac.Collection, pystac.Catalog)):
        demo_catalog.add_child(stac_object)

    catalog.normalize_hrefs(
        catalog_file,
        strategy=pystac.layout.BestPracticesLayoutStrategy()
    )
    catalog.save()


def uncatalog(product_id: str, *, catalog_file: PathLike[str]) -> None:
    raise NotImplementedError
