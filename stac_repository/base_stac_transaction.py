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
import tempfile
import shutil
from urllib.parse import urlparse, urljoin

from .stac import (
    StacIO,
    StacIOPerm,
    DefaultReadableStacIO,
    DefaultStacIO,
    Item,
    Collection,
    Catalog,
    load,
    load_parent,
    set_parent,
    set_version,
    unset_parent,
    save,
    delete,
    search,
    compute_extent,
    StacObjectError,
    HrefError,
    JSONObjectError
)

from .base_stac_commit import (
    ExtractError
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

    @abstractmethod
    def __init__(self, repository: "BaseStacRepository"):
        raise NotImplementedError

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
        *,
        catalog_assets: bool = False,
        catalog_assets_out_of_scope: bool = False,
        catalog_out_of_scope: bool = False,
        version: str
    ):
        """Catalogs a product. If parent_id is unspecified then the product is cataloged as the new root.

        Raises:
            FileNotFoundError: Product source does not exist
            StacObjectError: Product source is not a valid STAC object
            HrefError:  Product source href (scheme) cannot be processed
            UncatalogError:
            CatalogError:
        """

        if urlparse(product_file, scheme="").scheme == "":
            product_file = posixpath.abspath(product_file)
            product_base = posixpath.dirname(product_file)
        else:
            product_url = urlparse(product_file)
            product_base_url = product_url._replace(path=posixpath.dirname(product_url.path))
            product_base = product_base_url.geturl()

        scope_perm = StacIOPerm.R_STAC | (
            StacIOPerm.R_STAC if catalog_assets else StacIOPerm.NONE
        )

        out_of_scope_perm = StacIOPerm.NONE | (
            StacIOPerm.R_STAC if catalog_out_of_scope else StacIOPerm.NONE
        ) | (
            StacIOPerm.R_ASSETS if catalog_assets_out_of_scope else StacIOPerm.NONE
        )

        perms = {
            product_base: scope_perm,
            "/": out_of_scope_perm,
            "http://": out_of_scope_perm,
            "https://": out_of_scope_perm,
        }

        product = load(
            product_file,
            resolve_descendants=True,
            resolve_assets=True,
            io=DefaultReadableStacIO(perms),
        )

        unset_parent(product)

        set_version(product, version)

        if parent_id is None:
            if not isinstance(product, Catalog):
                raise CatalogError(f"Cannot catalog a {type(product).__class__} as root, only Catalog is permitted.")

            delete("/catalog.json", io=self)

            product.self_href = "/catalog.json"

            try:
                save(
                    product,
                    io=self
                )
            except HrefError as error:
                raise CatalogError(
                    f"{product.id} ({product_file}) couldn't be saved successfully : {str(error)}"
                ) from error
        else:
            try:
                self.uncatalog(product.id)
            except FileNotFoundError:
                pass

            parent = search(
                "/catalog.json",
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
                raise CatalogError(
                    f"{product.id} ({product_file}) couldn't be saved successfully : {str(error)}"
                ) from error

    def uncatalog(
        self,
        product_id: str,
        extract: Optional[str] = None
    ) -> Optional[str]:
        """Uncatalogs a product. If an extraction directory is specified then the uncataloged product is 
        saved into it.

        Raises:
            FileNotFoundError: Product couldn't be found in catalog
            UncatalogError: Product couldn't be deleted (in full, partial deletion may have occured)
            ExtractError: 
        """

        extract_file: Optional[str] = None

        product = search(
            "/catalog.json",
            product_id,
            io=self,
        )

        if product is None:
            raise FileNotFoundError(f"Product {product_id} not found in catalog")

        if extract is not None:
            extract = os.path.abspath(extract)

            try:
                os.makedirs(extract, exist_ok=True)

                if os.listdir(extract):
                    raise FileExistsError(f"{extract} is not empty.")
            except Exception as error:
                raise ExtractError(f"Couldn't create the extraction directory. {str(error)}") from error

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
                extract_file = os.path.join(extract, "catalog.json")
            elif isinstance(extracted_product, Collection):
                extract_file = os.path.join(extract, "collection.json")
            else:
                extract_file = os.path.join(extract, f"{extracted_product.id}.json")

            extracted_product.self_href = posixpath.abspath(extract_file)

            try:
                save(extracted_product, io=DefaultStacIO(perms={
                    posixpath.abspath(extract): StacIOPerm.RW_ANY
                }))
            except HrefError as error:
                shutil.rmtree(extract, ignore_errors=True)
                raise ExtractError(f"Couldn't save the extracted product. {str(error)}") from error

        try:
            parent = load_parent(product, io=self)
        except (HrefError) as error:
            logger.exception(
                f"[{type(error).__name__}] Skip recomputing parent child links as parent cannot be found inside the repository : {str(error)}")

            delete(product, io=self)
        else:

            if parent is None:
                delete(product, io=self)
            else:
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
                        logger.exception(
                            f"[{type(error).__name__}] Skipped recomputing ancestor extents : {str(error)}")
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

        return extract_file

    def recatalog(
        self,
        product_id: str,
        parent_id: Optional[str] = None,
    ):
        """Re-catalogs a product somewhere else. If parent_id is unspecified then the product is re-cataloged as the new root.

        Raises:
            TODO
        """

        with tempfile.TemporaryDirectory() as extract_dir:
            extract_file = self.uncatalog(product_id, extract=extract_dir)
            self.catalog(extract_file, parent_id=parent_id, catalog_assets=True)

    # def edit(
    #     self,
    #     product_id: str,
    #     edited: Union[Item, Collection, Catalog]
    # ):
    #     """Edits a product without mutating the catalog structure.

    #     To preserve the catalog integrity, some properties cannot be edited via this method :

    #     - type
    #     - id
    #     - assets
    #         Only assets with hrefs external to the repository can be modified
    #     - links
    #         Only links without the rel "self", "alternate", "root", "collection", "parent", "child" or "item"
    #         and with hrefs external to the repository can be modified
    #     - collection
    #     - geometry
    #     - bbox
    #     - extent
    #     - summaries

    #     Raises:
    #         NotImplementedError
    #     """

    #     raise NotImplementedError
