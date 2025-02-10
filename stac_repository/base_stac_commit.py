from __future__ import annotations
import datetime
import abc

import pystac
from .lib.stac import walk_stac_object
from .lib.stac import get_stac_object


class BaseStacCommit(metaclass=abc.ABCMeta):

    @property
    @abc.abstractmethod
    def parent(self) -> BaseStacCommit | None:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def catalog(self) -> pystac.Catalog:
        raise NotImplementedError

    @abc.abstractmethod
    def fetch(self, href: str, *, text: bool = True, hash: bool = False) -> str | bytes:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def id(self) -> str:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def datetime(self) -> datetime.datetime:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def message(self) -> str | None:
        raise NotImplementedError

    @property
    def objects(self):
        yield from walk_stac_object(self.catalog)

    @property
    def added_objects(self):
        if self.parent is None:
            yield from walk_stac_object(self.catalog)
            return

        for stac_object in walk_stac_object(self.catalog):
            if get_stac_object(self.parent.catalog, stac_object.id) is None:
                yield stac_object

    @property
    def removed_objects(self):
        if self.parent is None:
            return

        for stac_object in walk_stac_object(self.parent.catalog):
            if get_stac_object(self.catalog, stac_object.id) is None:
                yield stac_object

    @property
    def modified_objects(self):
        if self.parent is None:
            return

        for stac_object in walk_stac_object(self.catalog):
            parent_stac_object = get_stac_object(
                self.parent.catalog, stac_object.id)
            if parent_stac_object is None:
                continue

            if stac_object.to_dict(
                include_self_link=False
            ) != parent_stac_object.to_dict(
                include_self_link=False
            ):
                yield (stac_object, parent_stac_object)
                continue

            # if isinstance(stac_object, (pystac.Item, pystac.Collection)):
            #     for key in stac_object.assets.keys():
            #         asset_hash = self.fetch(
            #             stac_object.assets[key].href, hash=True)
            #         parent_asset_hash = self.parent.fetch(
            #             stac_object.assets[key].href, hash=True)

            #         if asset_hash != parent_asset_hash:
            #             yield (stac_object, parent_stac_object)
            #             break
