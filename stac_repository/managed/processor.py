from os import PathLike
import abc
from typing import Protocol, Iterator


class Processor(Protocol):

    __version__: str

    @staticmethod
    @abc.abstractmethod
    def discover(source: str) -> Iterator[str]:
        pass

    @staticmethod
    @abc.abstractmethod
    def id(product_source: str) -> str:
        pass

    @staticmethod
    @abc.abstractmethod
    def version(product_source: str) -> str:
        pass

    @staticmethod
    @abc.abstractmethod
    def process(product_source: str) -> PathLike[str]:
        pass

    @staticmethod
    @abc.abstractmethod
    def catalog(processed_product_stac_object_file: PathLike[str], *, catalog_file: PathLike[str]) -> None:
        pass

    @staticmethod
    @abc.abstractmethod
    def uncatalog(product_id: str, *, catalog_file: PathLike[str]) -> None:
        pass
