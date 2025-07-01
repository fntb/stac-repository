
import pytest

import os
import tempfile
import datetime
import random
import math
import shutil

from stac_repository.stac_commit_catalog import (
    StacCommitCatalogReader,
    StacCommitCatalogWriter
)

import pystac


class TestStacCommitCatalogReader(StacCommitCatalogReader):
    __test__ = False

    def get(self, href: str) -> pystac.Item | pystac.Collection | pystac.Catalog:
        for cls in (pystac.Item, pystac.Collection, pystac.Catalog):
            try:
                return cls.from_file(href)
            except ValueError:
                pass

        raise ValueError(f"{href} is not a valid STAC object")


@pytest.fixture
def item():
    dir = tempfile.mkdtemp(prefix=__name__)

    seed = random.random()

    item = pystac.Item(
        id := str(seed),
        geometry=None,
        bbox=(
            (seed * 358) - 179 - 1,
            (seed * 179) - 89 - 1,
            (seed * 358) - 179 + 1,
            (seed * 179) - 89 + 1,
        ),
        datetime=datetime.datetime.now(datetime.UTC) - datetime.timedelta(days=math.floor(seed * 365)),
        properties={
            "version": f"0.0.{math.ceil(seed * 100)}"
        }
    )

    item.set_self_href(href := os.path.join(dir, f"{id}.json"))

    yield item

    shutil.rmtree(dir, ignore_errors=True)


@pytest.fixture
def collection(item: pystac.Item):
    dir = tempfile.mkdtemp(prefix=__name__)

    seed = random.random()

    collection = pystac.Collection(
        str(seed),
        description=f"Seeded from {seed}",
        title=str(seed),
        extent=pystac.Extent.from_items([item]),
        extra_fields={
            "version": f"0.0.{math.ceil(seed * 100)}"
        }
    )

    collection.set_self_href(href := os.path.join(dir, "collection.json"))

    yield collection

    shutil.rmtree(dir, ignore_errors=True)


def test_get_object_version(item: pystac.Item, collection: pystac.Collection):
    catalog_reader = TestStacCommitCatalogReader()

    assert catalog_reader.get_object_version(item) is not None
    assert catalog_reader.get_object_version(collection) is not None


def test_walk_object_children(item: pystac.Item, collection: pystac.Collection):
    catalog_reader = TestStacCommitCatalogReader()

    assert len(list(catalog_reader._walk_object_children(item))) == 0
    assert len(list(catalog_reader._walk_object_children(collection))) == 1
