from __future__ import annotations

from typing import (
    Optional,
    Union,
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
from urllib.parse import urlparse, urljoin

from .stac import (
    StacIO,
    StacIOPerm,
    DefaultReadableStacIO,
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
    HrefError,
    JSONObjectError
)

if TYPE_CHECKING:
    from .base_stac_repository import (
        BaseStacRepository,
    )


logger = logging.getLogger(__file__)


class CatalogError(ValueError):
    """Generic fatal cataloging error"""
    pass


class UncatalogError(ValueError):
    """Generic fatal uncataloging error"""
    pass


class BaseStacTransaction(StacIO, metaclass=ABCMeta):

    _base_href: str
    """The base href of this repository.
    
    This value is used as the repository scope, stac objects and assets href falling outside of this scope will not be writeable
    and will need to be read from an external source.
    
    **This value must be a base uri or absolute posix path.**
    """

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
        catalog_assets: bool = False,
        catalog_assets_out_of_scope: bool = False,
        catalog_out_of_scope: bool = False
    ):
        """Catalogs a product.

        Raises:
            FileNotFoundError: Product source does not exist
            StacObjectError: Product source is not a valid STAC object
            HrefError:  Product source href (scheme) cannot be processed
            UncatalogError:
                - Old product version couldn't be deleted sucessfully
                - Cannot uncatalog the root
            CatalogError:
        """

        if urlparse(product_file, scheme="").scheme == "":
            product_file = posixpath.abspath(product_file)
            product_base = posixpath.dirname(product_file)
        else:
            product_url = urlparse(product_file)
            product_base_url = product_url._replace(path=posixpath.dirname(product_url.path))
            product_base = product_base_url.geturl()

        perms = {}

        if catalog_assets:
            perms = {
                **perms,
                product_base: StacIOPerm.R_ANY
            }

        if catalog_out_of_scope:
            perms = {
                **perms,
                "/": StacIOPerm.R_STAC,
                "http://": StacIOPerm.R_STAC,
                "https://": StacIOPerm.R_STAC,
            }

        if catalog_assets_out_of_scope:
            perms = {
                **perms,
                "/": StacIOPerm.R_ANY,
                "http://": StacIOPerm.R_ANY,
                "https://": StacIOPerm.R_ANY,
            }

        product = load(
            product_file,
            resolve_descendants=True,
            resolve_assets=True,
            io=DefaultReadableStacIO(perms),
        )

        unset_parent(product)

        try:
            self.uncatalog(product.id)
        except FileNotFoundError:
            pass

        if parent_id is None:
            try:
                parent = load(
                    self._catalog_href,
                    io=self,
                )
            except (FileNotFoundError, HrefError, StacObjectError) as error:
                raise CatalogError(f"Couldn't load catalog root : {str(error)}") from error
        else:
            parent = search(
                self._catalog_href,
                id=parent_id,
                io=self,
            )

            if parent is None:
                raise CatalogError(f"Parent {parent_id} not found in catalog")

        if isinstance(parent, Item):
            raise CatalogError(f"Cannot catalog under {parent_id}, this is an Item")

        set_parent(product, parent)

        last_ancestor: Union[Item, Collection, Catalog] = parent
        while True:
            if isinstance(last_ancestor, Collection):
                try:
                    last_ancestor.extent = compute_extent(last_ancestor, io=self)
                except StacObjectError as error:
                    logger.exception(f"[{type(error).__name__}] Skipped recomputing ancestor extents : {str(error)}")
                    break

            try:
                ancestor = load_parent(
                    last_ancestor,
                    io=self,
                )
            except (HrefError) as error:
                logger.exception(f"[{type(error).__name__}] Skipped recomputing ancestor extents : {str(error)}")
                break

            if ancestor is None:
                break
            else:
                last_ancestor = ancestor

        try:
            save(
                last_ancestor,
                io=self
            )
        except HrefError as error:
            raise CatalogError(
                f"{product.id} ({product_file}) couldn't be saved successfully : {str(error)}"
            ) from error

    def uncatalog(
        self,
        product_id: str,
    ):
        """Uncatalogs a product.

        Raises:
            FileNotFoundError: Product couldn't be found in catalog
            UncatalogError: 
                - Product couldn't be deleted (completely)
                - Cannot uncatalog the root
        """

        product = search(
            self._catalog_href,
            product_id,
            io=self,
        )

        if product is None:
            raise FileNotFoundError(f"Product {product_id} not found in catalog")

        try:
            parent = load_parent(product, io=self)
        except (HrefError) as error:
            logger.exception(
                f"[{type(error).__name__}] Skip recomputing parent child links as parent cannot be found inside the repository : {str(error)}")

            delete(product, io=self)
        else:
            if parent is None:
                raise UncatalogError(f"Cannot uncatalog the root")

            unset_parent(product)
            delete(product, io=self)

            last_ancestor: Union[Item, Collection, Catalog] = parent
            while True:
                if isinstance(last_ancestor, Collection):
                    try:
                        last_ancestor.extent = compute_extent(last_ancestor, io=self)
                    except StacObjectError as error:
                        logger.exception(
                            f"[{type(error).__name__}] Skipped recomputing ancestor extents : {str(error)}")
                        break

                try:
                    ancestor = load_parent(
                        last_ancestor,
                        io=self,
                    )
                except (HrefError) as error:
                    logger.exception(f"[{type(error).__name__}] Skipped recomputing ancestor extents : {str(error)}")
                    break

                if ancestor is None:
                    break
                else:
                    last_ancestor = ancestor

            try:
                save(
                    last_ancestor,
                    io=self
                )
            except HrefError as error:
                raise UncatalogError(f"{product_id} couldn't be deleted sucessfully : {str(error)}") from error
