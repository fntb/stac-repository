from typing import (
    Iterator,
    ClassVar
)

import os
import mimetypes
import uuid
import logging


from .stac import (
    load,
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
        source = os.path.abspath(source)

        def is_stac_file(file: str):
            if mimetypes.guess_type(file)[0] != "application/json":
                return False

            try:
                load(file)
            except StacObjectError as error:
                logger.info(f"Skipped {file} : {str(error)}")
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
        product_source = os.path.abspath(product_source)
        return load(product_source).id

    @staticmethod
    def version(product_source: str) -> str:
        product_source = os.path.abspath(product_source)
        try:
            return get_version(load(product_source))
        except VersionNotFoundError as error:
            logger.info(f"No version found {product_source}, generating random")
            return uuid.uuid4().hex

    @staticmethod
    def process(product_source: str) -> str:
        product_source = os.path.abspath(product_source)
        return os.path.abspath(product_source)
