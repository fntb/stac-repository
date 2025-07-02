from typing import (
    Iterator
)

import os
from os import PathLike
import mimetypes
import urllib.parse
import hashlib
import uuid

import pystac


from .stac_catalog import (
    make_from_file as _make_from_file,
    is_href_file as _is_href_file,
    get_version as _get_version,
    VersionNotFoundError
)


def _walk_stac_object_links(href: str) -> Iterator[str]:
    obj = _make_from_file(href)

    yield href

    if isinstance(obj, (pystac.Item, pystac.Collection)):
        for asset in obj.assets.values():
            yield urllib.parse.urljoin(href, asset.href)

    for link in obj.links:
        if link.rel in ["item", "child"]:
            yield from _walk_stac_object_links(urllib.parse.urljoin(href, link.href))


class NoneProcessor:

    __version__ = "0.0.1"

    @staticmethod
    def discover(source: str) -> Iterator[str]:
        def is_stac_file(file: str):
            if mimetypes.guess_type(file)[0] != "application/json":
                return False

            try:
                _make_from_file(file)
            except ValueError:
                return False

            return True

        if not os.path.lexists(source):
            return

        if os.path.isdir(source):
            for file_name in os.listdir(source):
                file = os.path.join(source, file_name)
                if is_stac_file(file):
                    yield file
        else:
            if is_stac_file(source):
                yield source

    @staticmethod
    def id(product_source: str) -> str:
        return pystac.STACObject.from_file(product_source).id

    @staticmethod
    def version(product_source: str) -> str:
        try:
            return _get_version(
                pystac.STACObject.from_file(product_source)
            )
        except VersionNotFoundError as error:

            # product_hash = hashlib.md5()

            # for file in _walk_stac_object_links(product_source):
            #     if _is_href_file(file):
            #         with open(file, "rb") as file_pipe:
            #             while file_chunk := file_pipe.read(65_536):
            #                 product_hash.update(file_chunk)

            # return product_hash.hexdigest()
            return uuid.uuid4().hex

    @staticmethod
    def process(product_source: str) -> PathLike[str]:
        return product_source
