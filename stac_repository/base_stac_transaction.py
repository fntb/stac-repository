from __future__ import annotations

from typing import (
    Optional,
    TYPE_CHECKING
)

from abc import (
    abstractmethod,
    ABCMeta
)

import contextlib
import os
import logging
import posixpath

from .stac import (
    StacIO,
    DefaultStacIO,
    Item,
    Collection,
    Catalog,
    load,
    load_parent,
    set_parent,
    unset_parent,
    save,
    delete,
    search,
    compute_extent,
    StacObjectError,
    JSONObjectError,
    FileNotInRepositoryError
)

if TYPE_CHECKING:
    from .base_stac_repository import BaseStacRepository


logger = logging.getLogger(__file__)


class ObjectNotFoundError(FileNotFoundError):
    pass


class ParentNotFoundError(ObjectNotFoundError):
    pass


class ParentCatalogError(ValueError):
    pass


class RootUncatalogError(ValueError):
    pass


class RootCatalogError(ValueError):
    pass


class BaseStacTransaction(StacIO, metaclass=ABCMeta):

    @abstractmethod
    def __init__(self, repository: "BaseStacRepository"):
        raise NotImplementedError

    @property
    def _catalog_href(self):
        return posixpath.join(self._base_href, "catalog.json")

    @contextlib.contextmanager
    def context(self, *, message: Optional[str] = None, **other_commit_args):
        try:
            yield self

            self.commit(
                **other_commit_args,
                message=message
            )
        except Exception as error:
            self.abort()
            raise error

    @abstractmethod
    def abort(self):
        """Aborts the transaction in progress (i.e. rollback all changes made to the catalog since the last commit)"""
        raise NotImplementedError

    @abstractmethod
    def commit(self, *, message: Optional[str] = None):
        """Commits the transaction in progress (i.e. confirms all changes made to the catalog up to this point)"""
        raise NotImplementedError

    def catalog(
        self,
        product_file: str,
        parent_id: Optional[str] = None,
    ):
        """Catalogs a product.

        Raises:
            ObjectNotFoundError: Product does not exist or is outside of repository
            StacObjectError: Product is not a valid STAC Object
            ParentNotFoundError: Parent not found in catalog
            ParentCatalogError: Parent not suitable (Item)
            RootCatalogError: Product has the same id as the root and would thus replace it
        """

        product_store = DefaultStacIO(base_href=os.path.dirname(product_file))

        try:
            product = load(
                product_file,
                resolve_descendants=True,
                resolve_assets=True,
                store=product_store
            )
        except FileNotFoundError as error:
            raise ObjectNotFoundError(f"{product_file} does not exist") from error
        except StacObjectError as error:
            raise StacObjectError(f"{product_file} is not a valid STAC Object") from error
        except FileNotInRepositoryError as error:
            raise ObjectNotFoundError(
                f"{product_file} cannot be retrieved outside of repository '{os.path.dirname(product_file)}'"
            ) from error

        unset_parent(product)

        try:
            self.uncatalog(product.id)
        except ObjectNotFoundError:
            pass

        if parent_id is None:
            try:
                parent = load(
                    self._catalog_href,
                    store=self,
                )
            except (FileNotFoundError, FileNotInRepositoryError, StacObjectError) as error:
                raise ParentNotFoundError(f"Catalog root not found") from error
        else:
            parent = search(
                self._catalog_href,
                id=parent_id,
                store=self
            )

        if parent is None:
            raise ParentNotFoundError(f"Parent {parent_id} not found in catalog")

        if isinstance(parent, Item):
            raise ParentCatalogError(f"Cannot catalog under {parent_id}, this is an Item")

        set_parent(product, parent)

        last_ancestor: Item | Collection | Catalog = parent
        while True:
            try:
                last_ancestor.extent = compute_extent(last_ancestor, store=self)
            except StacObjectError as error:
                logger.exception(f"[{type(error).__name__}] Skipped recomputing ancestor extents : {str(error)}")
                break

            try:
                ancestor = load_parent(
                    last_ancestor,
                    store=self,
                )
            except FileNotInRepositoryError as error:
                logger.exception(f"[{type(error).__name__}] Skipped recomputing ancestor extents : {str(error)}")
                break

            if ancestor is None:
                break
            else:
                last_ancestor = ancestor

        save(
            last_ancestor,
            store=self
        )

    def uncatalog(
        self,
        product_id: str,
    ):
        """Uncatalogs a product.

        Raises:
            ObjectNotFoundError
            ParentNotFoundError
            RootUncatalogError: The product is the catalog root
        """

        product = search(
            self._catalog_href,
            product_id,
            store=self
        )

        if product is None:
            raise ObjectNotFoundError(f"Product {product_id} not found in catalog")

        try:
            parent = load_parent(product, store=self)
        except FileNotInRepositoryError as error:
            raise ParentNotFoundError("Parent not found in catalog") from error

        if parent is None:
            raise RootUncatalogError(f"Cannot uncatalog the root")

        unset_parent(product)
        delete(product, store=self)

        last_ancestor: Item | Collection | Catalog = parent
        while True:
            try:
                last_ancestor.extent = compute_extent(last_ancestor, store=self)
            except StacObjectError as error:
                logger.exception(f"[{type(error).__name__}] Skipped recomputing ancestor extents : {str(error)}")
                break

            try:
                ancestor = load_parent(
                    last_ancestor,
                    store=self,
                )
            except FileNotInRepositoryError as error:
                logger.exception(f"[{type(error).__name__}] Skipped recomputing ancestor extents : {str(error)}")
                break

            if ancestor is None:
                break
            else:
                last_ancestor = ancestor

        save(
            last_ancestor,
            store=self
        )
