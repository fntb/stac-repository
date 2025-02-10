from typing import Any, Iterator
import abc
import os
from os import path
import shutil
import json
import datetime

import pystac

from .generate_id import generate_id
from .generate_geometry import generate_geometry
from .generate_image import generate_random_walk_image
from .generate_metadata import generate_metadata


class Product(metaclass=abc.ABCMeta):

    _product_source: str

    @staticmethod
    @abc.abstractmethod
    def generate(product_source: str) -> None:
        pass

    @staticmethod
    @abc.abstractmethod
    def discover(source: str) -> Iterator[str]:
        pass

    @staticmethod
    @abc.abstractmethod
    def is_product_source(product_source: str) -> bool:
        pass

    def __init__(self, product_source: str):
        self._product_source = product_source

    @property
    @abc.abstractmethod
    def id(self) -> str:
        pass

    @property
    @abc.abstractmethod
    def version(self) -> str:
        pass

    @abc.abstractmethod
    def to_stac_object(self) -> pystac.STACObject:
        pass


class SimpleProduct(Product):

    _metadata: Any

    @staticmethod
    def generate(product_source):
        try:
            os.makedirs(product_source, exist_ok=True)

            with open(path.join(product_source, "metadata.json"), "w") as metadata_file_pipe:
                json.dump({
                    "id": generate_id(),
                    "properties": {
                        **generate_metadata(),
                        "version": "0.0.1",
                        "datetime": datetime.datetime.now().isoformat(),
                        "geometry": generate_geometry()
                    }
                }, metadata_file_pipe)

            generate_random_walk_image(product_source)
        except Exception as error:
            shutil.rmtree(product_source)
            raise error

    @staticmethod
    def is_product_source(product_source: str) -> bool:
        for file_name in os.listdir(product_source):
            if file_name == "metadata.json":
                break
        else:
            return False

        return True

    @staticmethod
    def discover(source):
        def _discover_metadata_file(dir: str) -> Iterator[str]:
            if SimpleProduct.is_product_source(dir):
                yield dir
            else:
                for file_name in os.listdir(dir):
                    file = path.join(dir, file_name)

                    if path.isdir(file):
                        yield from _discover_metadata_file(file)

        yield from _discover_metadata_file(source)

    def __init__(self, product_source: str):
        super().__init__(product_source)

        with open(path.join(self._product_source, "metadata.json")) as metadata_file_pipe:
            self._metadata = json.load(metadata_file_pipe)

    @property
    def id(self):
        return self._metadata["id"]

    @property
    def version(self):
        return self._metadata["properties"]["version"]

    def to_stac_object(self):
        item = pystac.Item(
            self.id,
            self._metadata["properties"]["geometry"],
            None,
            datetime=datetime.datetime.fromisoformat(
                self._metadata["properties"]["datetime"]),
            properties={
                **self._metadata
            }
        )

        for file_name in os.listdir(self._product_source):
            file = path.join(self._product_source, file_name)
            if path.isfile(file):
                item.add_asset(
                    file_name,
                    pystac.Asset(
                        file,
                        file_name
                    )
                )

        return item


class ProductFactory():

    @staticmethod
    def discover(source: str) -> Iterator[str]:
        yield from SimpleProduct.discover(source)

    @staticmethod
    def make(product_source: str) -> Product:
        if SimpleProduct.is_product_source(product_source):
            return SimpleProduct(product_source)
        else:
            raise ValueError(product_source)
