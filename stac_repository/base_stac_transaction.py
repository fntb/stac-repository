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

from .stac import (
    Item,
    Collection,
    Catalog,
    Link,
    Asset,
    MimeTypes,
    load,
    load_parent,
    save,
    delete,
    search,
    compute_extent,
    is_in_scope,
    urlparse,
    urljoin
)


class ObjectNotFoundError(FileNotFoundError):
    ...


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
        catalog_scope = os.path.dirname(self._root_catalog_href)

        product = load(
            product_file,
            recursive=True,
            scope=os.path.dirname(product_file)
        )

        product.links = [
            link
            for link in product.links
            if link.rel not in ("parent", "root", "self")
        ]

        if parent_id is None:
            parent = load(
                self._root_catalog_href,
                store=self
            )
        else:
            parent = search(
                self._root_catalog_href,
                id=parent_id,
                scope=catalog_scope,
                store=self
            )

        if parent is None:
            raise ParentNotFoundError(f"Parent {parent_id} not found in catalog")

        if isinstance(parent, Item):
            raise ParentCatalogError(f"Cannot catalog under {parent_id}, this is an Item")

        try:
            self.uncatalog(product.id)
        except ObjectNotFoundError:
            pass

        product_link_href: str
        product_link_rel: str
        if isinstance(product, Item):
            product_link_href = urljoin(product.id, f"{product.id}.json")
            product_link_rel = "item"
        elif isinstance(product, Collection):
            product_link_href = urljoin(product.id, "collection.json")
            product_link_rel = "child"
        else:
            product_link_href = urljoin(product.id, "catalog.json")
            product_link_rel = "child"

        for link in parent.links:
            if link.href == product_link_href:
                break
        else:
            parent.links.append(Link(
                href=product_link_href,
                rel=product_link_rel,
                type=MimeTypes.json,
                _target=product
            ))

        parent_link_href: str
        if isinstance(parent, Collection):
            parent_link_href = urljoin("..", "collection.json")
        else:
            parent_link_href = urljoin("..", "catalog.json")

        product.links.append(Link(
            href=parent_link_href,
            rel="parent",
            type=MimeTypes.json,
        ))

        def normalize_link(link: Link) -> Link | None:
            if link.rel in ["self", "alternate"]:
                return None

            if link.rel not in ["child", "item"]:
                return link

            if link.target is None:
                return link

            child = link.target

            if isinstance(child, Item):
                link.href = os.path.join(child.id, f"{child.id}.json")
            elif isinstance(child, Collection):
                link.href = os.path.join(child.id, f"collection.json")
            else:
                link.href = os.path.join(child.id, f"catalog.json")

            normalize(child)

            return link

        def normalize_asset(asset: Asset) -> Asset | None:
            if is_in_scope(asset.href, scope=catalog_scope):
                asset.target = asset.href
                asset.href = os.path.basename(urlparse(asset.href).path)

            return asset

        def normalize(stac_object: Item | Collection | Catalog):
            stac_object.links = [
                normalized_link
                for link in stac_object.links
                if (normalized_link := normalize_link(link)) is not None
            ]

            if isinstance(stac_object, (Item, Collection)) and stac_object.assets:
                stac_object.assets = {
                    key: normalized_asset
                    for (key, asset) in stac_object.assets.items()
                    if (normalized_asset := normalize_asset(asset)) is not None
                }

            for link in stac_object.links:
                if link.target is not None:
                    link.target.target = urljoin(stac_object.target, link.href)

                    normalize(link.target)

        normalize(parent)

        while True:
            parent.extent = compute_extent(parent, scope=catalog_scope, store=self)

            grand_parent = load_parent(
                parent,
                store=self
            )

            if grand_parent is None:
                break
            else:
                parent = grand_parent

        save(
            parent,
            store=self
        )

    def uncatalog(
        self,
        product_id: str,
    ):
        """Uncatalogs a product.

        Raises:
            RootUncatalogError: The product is the catalog root
            StacObjectError: Cannot compute the parent new extent
            ObjectNotFoundError
        """

        catalog_scope = os.path.dirname(self._root_catalog_href)

        product = search(
            self._root_catalog_href,
            product_id,
            scope=catalog_scope,
            store=self
        )

        if product is None:
            raise ObjectNotFoundError(f"Product {product_id} not found in catalog")

        for link in product.links:
            if link.rel == "parent":
                parent_link = link
                break
        else:
            raise RootUncatalogError(f"Cannot uncatalog the root")

        parent = load(
            parent_link.href,
            store=self
        )

        parent.links = [link for link in parent.links if link.href != product.target]

        delete(product, scope=catalog_scope, store=self)

        while True:
            parent.extent = compute_extent(parent, scope=catalog_scope, store=self)

            grand_parent = load_parent(
                parent,
                store=self
            )

            if grand_parent is None:
                break
            else:
                parent = grand_parent

        save(
            parent,
            store=self
        )
