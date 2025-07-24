from typing import (
    Iterator,
    ClassVar
)

import os
import mimetypes
import uuid
import logging
import posixpath
from urllib.parse import urlparse


from .stac import (
    load,
    DefaultReadableStacIO,
    StacIOPerm,
    get_version,
    VersionNotFoundError,
    StacObjectError
)

from .processor import Processor

logger = logging.getLogger(__file__)


class StacProcessor(Processor):

    __version__: ClassVar[str] = "0.0.1"

    @staticmethod
    def discover(source: str) -> Iterator[str]:
        def is_file(file: str) -> bool:
            return urlparse(file, scheme="").scheme == ""

        def is_stac_file(file: str):
            if mimetypes.guess_type(file)[0] != "application/json":
                return False

            try:
                load(
                    file,
                    io=DefaultReadableStacIO({
                        posixpath.abspath(file): StacIOPerm.R_STAC
                    })
                )
            except StacObjectError as error:
                logger.info(f"Skipped {file} : {str(error)}")
                return False

            return True

        if is_file(source):
            source = os.path.abspath(source)

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
        else:
            if is_stac_file(source):
                yield source

    @staticmethod
    def id(product_source: str) -> str:
        if urlparse(product_source, scheme="").scheme == "":
            product_source = os.path.abspath(product_source)

        return load(
            product_source,
            io=DefaultReadableStacIO({
                posixpath.abspath(product_source): StacIOPerm.R_STAC
            })
        ).id

    @staticmethod
    def version(product_source: str) -> str:
        if urlparse(product_source, scheme="").scheme == "":
            product_source = os.path.abspath(product_source)

        try:
            return get_version(
                load(
                    product_source,
                    io=DefaultReadableStacIO({
                        posixpath.abspath(product_source): StacIOPerm.R_STAC
                    })
                )
            )
        except VersionNotFoundError as error:
            logger.info(f"No version found {product_source}, use id as version")
            return StacProcessor.id(product_source)

    @staticmethod
    def process(product_source: str) -> str:
        if urlparse(product_source, scheme="").scheme == "":
            product_source = os.path.abspath(product_source)

        return product_source
