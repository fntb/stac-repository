from os import PathLike
from typing import Protocol, Iterator


class Processor(Protocol):

    __version__: str

    @staticmethod
    def discover(source: str) -> Iterator[str]:
        """_Discover products from a source._

        Args:
            source: _A data source, typically an uri or directory path depending on where the products are stored_

        Returns:
            _Yields product sources, typically uris or file paths_
        """

        pass

    @staticmethod
    def id(product_source: str) -> str:
        """_Get the id of a product_

        Args:
            product_source: _The product source, typically an uri or file path_

        Returns:
            _The product id_
        """

        pass

    @staticmethod
    def version(product_source: str) -> str:
        """_Get a product version_

        Args:
            product_source: _The product source, typically an uri or file path_

        Returns:
            _The product version_
        """

        pass

    @staticmethod
    def process(product_source: str) -> PathLike[str]:
        """_Process a product into a STAC object (item, collection, or even catalog)_

        Args:
            product_source: _The product source, typically an uri or file path_

        Returns:
            _The path to the processed STAC object_
        """

        pass
