from __future__ import annotations
from typing import (
    Optional,
    TYPE_CHECKING,
    Union,
    Type,
)


import datetime
import posixpath
import shutil
import os
from abc import abstractmethod, ABCMeta

from .stac import (
    Item,
    Collection,
    Catalog,
    ReadableStacIO,
    DefaultStacIO,
    StacIOPerm,
    search,
    load,
    unset_parent,
    StacObjectError,
    JSONObjectError,
    HrefError,
    save
)

if TYPE_CHECKING:
    from .base_stac_repository import BaseStacRepository


class BackupValueError(ValueError):
    """Backend cannot process this type of backup destination."""
    pass


class ExtractError(Exception):
    """Generic fatal extraction error"""
    pass


class BaseStacCommit(ReadableStacIO, metaclass=ABCMeta):

    @abstractmethod
    def __init__(self, repository: "BaseStacRepository"):
        raise NotImplementedError

    @property
    @abstractmethod
    def id(self) -> str:
        raise NotImplementedError

    @property
    @abstractmethod
    def datetime(self) -> datetime.datetime:
        raise NotImplementedError

    @property
    def message(self) -> Union[str, Type[NotImplementedError]]:
        return NotImplementedError

    @property
    @abstractmethod
    def parent(self) -> Optional[BaseStacCommit]:
        raise NotImplementedError

    def rollback(self) -> Optional[Type[NotImplementedError]]:
        """Rollback the repository to this commit.

        Returns:
            NotImplementedError: If the concrete implementation does not support rollbacks.
        """
        return NotImplementedError

    def backup(self, backup_url: str) -> Optional[Type[NotImplementedError]]:
        """Backup the repository as it was in this commit.

        Returns:
            NotImplementedError: If the concrete implementation does not support backups.

        Raises:
            BackupValueError: If the backup_url is not valid
        """
        return NotImplementedError

    def extract(self, export_dir: str, product_id: Optional[str]):
        """Extracts the catalog, or a product, as it was in this commit.

        Raises:
            FileNotFoundError:
            FileExistsError:
            StacObjectError:
            HrefError:
        """
        export_dir = os.path.abspath(export_dir)

        try:
            os.makedirs(export_dir, exist_ok=True)

            if os.listdir(export_dir):
                raise FileExistsError(f"{export_dir} is not empty.")
        except Exception as error:
            raise ExtractError(f"Couldn't create the extraction directory. {str(error)}") from error

        product = search(
            "/catalog.json",
            product_id,
            io=self,
        )

        if product is None:
            raise FileNotFoundError(f"Product {product_id} not found in catalog")

        try:
            extracted_product = load(
                product.self_href,
                resolve_descendants=True,
                resolve_assets=True,
                io=self
            )
        except (StacObjectError, HrefError) as error:
            raise ExtractError(str(error)) from error

        unset_parent(extracted_product)

        if isinstance(extracted_product, Catalog):
            extract_file = os.path.join(export_dir, "catalog.json")
        elif isinstance(extracted_product, Collection):
            extract_file = os.path.join(export_dir, "collection.json")
        else:
            extract_file = os.path.join(export_dir, f"{extracted_product.id}.json")

        extracted_product.self_href = posixpath.abspath(extract_file)

        try:
            save(extracted_product, io=DefaultStacIO(perms={
                posixpath.abspath(export_dir): StacIOPerm.RW_ANY
            }))
        except HrefError as error:
            shutil.rmtree(export_dir, ignore_errors=True)
            raise ExtractError(f"Couldn't save the extracted product. {str(error)}") from error

    def search(
        self,
        id: str,
    ) -> Optional[Union[Item, Collection, Catalog]]:
        """Searches the cataloged object `id`.

        This method will **not** lookup objects outside of the repository.
        """
        try:
            self.get("/catalog.json")
        except FileNotFoundError:
            return None
        except Exception:
            pass

        return search(
            "/catalog.json",
            id=id,
            io=self,
        )
