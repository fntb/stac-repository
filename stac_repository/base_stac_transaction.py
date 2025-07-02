from __future__ import annotations

from typing import (
    Optional
)

from abc import (
    abstractmethod,
    ABCMeta
)

import contextlib
import os
import io

import pystac
import pystac.layout
import pystac.stac_io

import json

from .base_stac_commit import BaseStacCommit

from .stac_catalog import (
    get_child as _get_child,
    WalkedStacObject,
    ObjectNotFoundError,
    compute_extent as _compute_extent,
    is_href_file as _is_href_file,
    StacObjectError,
    make_from_file as _make_from_file
)


class ParentNotFoundError(ObjectNotFoundError):
    pass


class ParentCatalogError(ValueError):
    pass


class RootUncatalogError(ValueError):
    pass


class TransactionStacIO(pystac.stac_io.DefaultStacIO):

    _transaction: BaseStacTransaction

    def __init__(self, *args: pystac.Any, transaction: BaseStacTransaction, **kwargs: pystac.Any):
        super().__init__(*args, **kwargs)
        self._transaction = transaction

    def read_text_from_href(self, href: str) -> str:
        return self._transaction.get(href)

    def write_text_to_href(self, href: str, txt: str) -> None:
        return self._transaction.set(href, txt)


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
    def set(self, href: str, value: str):
        raise NotImplementedError

    @abstractmethod
    def set_asset(self, href: str, asset: io.BytesIO | io.StringIO | bytes | str):
        raise NotImplementedError

    @abstractmethod
    def unset(self, href: str):
        """Delete the STAC object at href, all its descendants, and all their assets"""
        raise NotImplementedError

    @property
    def io(self) -> TransactionStacIO:
        return TransactionStacIO(transaction=self)

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
                self._root_catalog_href,
                _make_from_file(self._root_catalog_href, self.io),
                []
            )
        else:
            try:
                (parent_href, parent, ancestry) = _get_child(
                    id=parent_id,
                    href=self._root_catalog_href,
                    stac_io=self.io,
                    domain=os.path.dirname(self._root_catalog_href)
                )
            except ObjectNotFoundError as error:
                raise ParentNotFoundError(f"Parent {parent_id} not found in catalog") from error

        if isinstance(parent, pystac.Item):
            raise ParentCatalogError(f"Cannot catalog under {parent_id}, this is an Item")

        # Uncatalog the old version of the product, if any
        try:
            self.uncatalog(product.id)
        except ObjectNotFoundError:
            pass

        # Separate product from its ascendants, if any
        product.set_parent(None)
        product.set_root(None)

        # Resolve descendants (and only descendants) into memory
        # Preserve the assets absolute hrefs
        product.resolve_links()
        if isinstance(product, pystac.Item):
            product.make_asset_hrefs_absolute()
        else:
            product.make_all_asset_hrefs_absolute()

        # Join product to the catalog (parent and root) and normalize all descendants hrefs
        if not isinstance(product, pystac.Item):
            parent.add_child(
                product,
                title=product.title,
                set_parent=True
            )
        else:
            parent.add_item(
                product,
                product.common_metadata.title,
                set_parent=True
            )

        # Compute the parent new extent and summary
        # TODO : Recompute each ancestor instead
        if isinstance(parent, pystac.Collection):
            parent.extent = _compute_extent(obj=parent, href=parent_href, stac_io=self.io)

        # Save the parent
        # TODO : Save each ancestor instead
        parent.normalize_and_save(
            self._root_catalog_href,
            strategy=pystac.layout.BestPracticesLayoutStrategy(),
            catalog_type=pystac.CatalogType.SELF_CONTAINED,
            stac_io=self.io,
            skip_unresolved=True
        )

        # Recursively save the product assets
        def save_assets(obj: pystac.Item | pystac.Collection | pystac.Catalog):
            # identify local assets and copy them into the catalog while updating the node asset href
            if isinstance(obj, (pystac.Item, pystac.Collection)):
                for asset in obj.assets.values():
                    if _is_href_file(asset.href):
                        relative_cataloged_asset_href = os.path.basename(asset.href)
                        absolute_cataloged_asset_href = os.path.join(
                            os.path.dirname(asset.owner.self_href),
                            relative_cataloged_asset_href
                        )

                        with open(asset.href, "rb") as asset_stream:
                            self.set_asset(absolute_cataloged_asset_href, asset_stream)

                        asset.href = relative_cataloged_asset_href

            if isinstance(obj, (pystac.Catalog, pystac.Collection)):
                for child in obj.get_children():
                    save_assets(child)
                for item in obj.get_items():
                    save_assets(item)

        save_assets(product)

        # Save the modified asset links
        if isinstance(product, pystac.Item):
            product.save_object(stac_io=self.io, include_self_link=False)
        else:
            product.save(stac_io=self.io)

    def uncatalog(
        self,
        product_id: str,
    ):
        """Uncatalogs a product.

        Raises:
            RootUncatalogError: The product is the catalog root
            StacObjectError: Cannot compute the parent new extent
        """
        (product_href, product, ancestry) = _get_child(
            id=product_id,
            href=self._root_catalog_href,
            stac_io=self.io,
            domain=os.path.dirname(self._root_catalog_href)
        )

        if not ancestry:
            raise RootUncatalogError(f"Cannot uncatalog the root")

        self.unset(product_href)

        (parent_href, parent) = ancestry[0]
        if isinstance(product, pystac.Item):
            parent.remove_item(product_id)
        else:
            parent.remove_child(product_id)

        if isinstance(parent, pystac.Collection):
            parent.extent = _compute_extent(obj=parent, href=parent_href, stac_io=self.io)

        parent.save(stac_io=self.io)
