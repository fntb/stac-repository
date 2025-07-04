from typing import (
    Iterator
)

import os
from os import PathLike
import mimetypes
import uuid


from .stac import (
    load,
    get_version,
    VersionNotFoundError,
    StacObjectError
)


class NoneProcessor:

    __version__ = "0.0.1"

    @staticmethod
    def discover(source: str) -> Iterator[str]:
        def is_stac_file(file: str):
            if mimetypes.guess_type(file)[0] != "application/json":
                return False

            try:
                load(file)
            except StacObjectError:
                return False

            return True

        if not os.path.lexists(source):
            return

        source = os.path.abspath(source)

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
        return load(product_source).id

    @staticmethod
    def version(product_source: str) -> str:
        try:
            return get_version(load(product_source))
        except VersionNotFoundError as error:
            return uuid.uuid4().hex

    @staticmethod
    def process(product_source: str) -> PathLike[str]:
        return os.path.abspath(product_source)
