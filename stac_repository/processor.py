from typing import (
    Iterator,
    ClassVar
)

import sys

if sys.version_info >= (3, 9):
    from typing import Protocol
else:
    from typing_extensions import Protocol


class Processor(Protocol):

    __version__: ClassVar[str]

    @staticmethod
    def discover(source: str) -> Iterator[str]:
        """_Discover products from a source._

        Args:
            source: _A data source, typically an uri or directory path depending on where the products are stored_

        Returns:
            _Yields product sources, typically uris or file paths_
        """

        raise NotImplementedError

    @staticmethod
    def id(product_source: str) -> str:
        """_Get the id of a product_

        Args:
            product_source: _The product source, typically an uri or file path_

        Returns:
            _The product id_
        """

        raise NotImplementedError

    @staticmethod
    def version(product_source: str) -> str:
        """_Get a product version_

        Args:
            product_source: _The product source, typically an uri or file path_

        Returns:
            _The product version_
        """

        raise NotImplementedError

    @staticmethod
    def process(product_source: str) -> str:
        """_Process a product into a STAC object (item, collection, or even catalog)_

        Args:
            product_source: _The product source, typically an uri or file path_

        Returns:
            _The path to the processed STAC object_
        """

        raise NotImplementedError
