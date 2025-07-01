from typing import (
    Optional
)

from abc import (
    abstractmethod,
    ABCMeta
)

import contextlib
import os

import pystac

from .base_stac_commit import BaseStacCommit

from .stac_catalog import (
    get_child as _get_child,
    WalkedStacObject,
    ObjectNotFoundError as _ObjectNotFoundError,
    compute_extent as _compute_extent,
    is_href_file as _is_href_file,
    StacObjectError
)


class ParentNotFoundError(_ObjectNotFoundError):
    pass


class ParentCatalogError(ValueError):
    pass


class RootUncatalogError(ValueError):
    pass


class BaseStacTransaction(BaseStacCommit, metaclass=ABCMeta):

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
    def set(self, href: str, value: pystac.Item | pystac.Catalog):
        raise NotImplementedError

    @abstractmethod
    def set_asset(self, href: str, asset_file: str):
        raise NotImplementedError

    @abstractmethod
    def unset(self, href: str):
        """Delete the STAC object at href, all its descendants, and all their assets"""
        raise NotImplementedError

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
            StacObjectError: The product is not a valid Item or Collection, or cannot compute the parent new extent
            ParentNotFoundError
            ParentCatalogError: The parent is an Item
        """
        product: pystac.Item | pystac.Collection | None = None

        for cls in (pystac.Item, pystac.Collection):
            try:
                product = cls.from_file(product_file)
            except pystac.STACTypeError:
                pass

        if product is None:
            raise StacObjectError(f"{product_file} is not a valid STAC Item or Collection")

        if parent_id is None:
            (parent_href, parent, ancestry) = WalkedStacObject(
                self._root_catalog_href, self.get(self._root_catalog_href), [])
        else:
            try:
                (parent_href, parent, ancestry) = _get_child(
                    id=parent_id,
                    href=self._root_catalog_href,
                    factory=self.get,
                    domain=self._root_catalog_href
                )
            except _ObjectNotFoundError as error:
                raise ParentNotFoundError(f"Parent {parent_id} not found in catalog") from error

        if isinstance(parent, pystac.Item):
            raise ParentCatalogError(f"Cannot catalog under {parent_id}, this is an Item")

        # Uncatalog the old version of the product, if any
        self.uncatalog(product.id)

        # Separate product from its ascendants, if any
        product.set_parent(None)
        product.set_root(None)

        # Resolve descendants (and only descendants) into memory
        # Preserve the assets absolute hrefs
        product.resolve_links()
        product.make_all_asset_hrefs_absolute()

        # Join product to the catalog (parent and root) and normalize all descendants hrefs
        product.set_parent(parent)
        product.normalize_hrefs(
            self._root_catalog_href,
            strategy=pystac.layout.BestPracticesLayoutStrategy(),
            skip_unresolved=True
        )

        # Compute the parent new extent and summary
        if isinstance(parent, pystac.Collection):
            parent.extent = _compute_extent(obj=parent, href=parent_href, factory=self.get)

        # Recursively save_object the parent, the product and all its descendants
        def set_product(obj: pystac.Item | pystac.Collection | pystac.Catalog):
            self.set(obj.self_href, obj)

            if isinstance(obj, (pystac.Catalog, pystac.Collection)):
                for child in obj.get_children():
                    set_product(child)
                for item in obj.get_items():
                    set_product(item)

        self.set(parent_href, parent)
        set_product(product)

        # Walk each node of the product and a copy of the source product,
        # identify local assets and copy them into the catalog while updating the node asset href
        def walk_product_assets(
            obj: pystac.Item | pystac.Collection | pystac.Catalog
        ):
            if isinstance(obj, (pystac.Item, pystac.Collection)):
                for asset in obj.assets.values():
                    yield asset

            if isinstance(obj, pystac.Catalog):
                for item in obj.get_items():
                    yield from walk_product_assets(item)
                for child in obj.get_children():
                    yield from walk_product_assets(child)

        for asset in walk_product_assets(product):
            if _is_href_file(asset.href):
                asset_file = asset.href
                cataloged_asset_href = os.path.join(
                    os.path.dirname(asset.owner.self_href),
                    os.path.basename(asset_file)
                )

                self.set_asset(cataloged_asset_href, asset_file)

        # Recursively save_object the product and all its descendants
        set_product(product)

    def uncatalog(
        self,
        product_id: str,
    ):
        """Uncatalogs a product.

        Raises:
            RootUncatalogError: The product is the catalog root
            StacObjectError: Cannot compute the parent new extent
        """
        try:
            (product_href, product, ancestry) = _get_child(
                id=product_id,
                href=self._root_catalog_href,
                factory=self.get,
                domain=self._root_catalog_href
            )
        except _ObjectNotFoundError as error:
            return

        if not ancestry:
            raise RootUncatalogError(f"Cannot uncatalog the root")

        self.unset(product_href)

        (parent_href, parent) = ancestry[0]
        if isinstance(product, pystac.Item):
            parent.remove_item(product_id)
        else:
            parent.remove_child(product_id)

        if isinstance(parent, pystac.Collection):
            parent.extent = _compute_extent(obj=parent, href=parent_href, factory=self.get)

        self.set(parent_href, parent)
