from os import PathLike
import abc
from typing import Protocol, Iterator


class Processor(Protocol):

    __version__: str

    @staticmethod
    @abc.abstractmethod
    def discover(source: str) -> Iterator[str]:
        """_Discover products from a source._

        Args:
            source: _A data source, typically an uri or directory path depending on where the products are stored_

        Returns:
            _Yields product sources, typically uris or file paths_
        """

        pass

    @staticmethod
    @abc.abstractmethod
    def id(product_source: str) -> str:
        """_Get the id of a product_

        Args:
            product_source: _The product source, typically an uri or file path_

        Returns:
            _The product id_
        """

        pass

    @staticmethod
    @abc.abstractmethod
    def version(product_source: str) -> str:
        """_Get a product version_

        Args:
            product_source: _The product source, typically an uri or file path_

        Returns:
            _The product version_
        """

        pass

    @staticmethod
    @abc.abstractmethod
    def process(product_source: str) -> PathLike[str]:
        """_Process a product into a STAC object (item, collection, or even catalog)_

        Args:
            product_source: _The product source, typically an uri or file path_

        Returns:
            _The path to the processed STAC object_
        """

        pass

    @staticmethod
    @abc.abstractmethod
    def catalog(processed_product_stac_object_file: PathLike[str], *, catalog_file: PathLike[str]) -> None:
        """_Insert a processed product (STAC object) in the root STAC catalog_

        Args:
            processed_product_stac_object_file: _The path to the processed product STAC file_
            catalog_file: _The path to the root STAC catalog_
        """

        pass

    @staticmethod
    @abc.abstractmethod
    def uncatalog(product_id: str, *, catalog_file: PathLike[str]) -> None:
        """_Remove a cataloged product (STAC object) from the root STAC catalog_

        Args:
            product_id: _The product id, which is also the id of the STAC object to remove_
            catalog_file: _The path to the root STAC catalog_
        """

        pass
