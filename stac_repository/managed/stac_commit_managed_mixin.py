

from __future__ import annotations

from functools import cached_property
from typing import Iterator

import pystac

from ..base_stac_commit import BaseStacCommit
from .stac_extension import StacRepositoryExtension


class StacCommitManagedMixin(BaseStacCommit):

    @property
    def parent(self):
        return super().parent

    @property
    def products(self) -> Iterator[pystac.STACObject]:
        for stac_object in self.objects:
            if StacRepositoryExtension.implements(stac_object):
                yield stac_object

    @property
    def ingested_products(self) -> Iterator[pystac.STACObject]:
        for stac_object in self.added_objects:
            if StacRepositoryExtension.implements(stac_object):
                yield stac_object

    @property
    def reprocessed_products(self) -> Iterator[tuple[pystac.STACObject, pystac.STACObject]]:
        for stac_object in self.modified_objects:
            if StacRepositoryExtension.implements(stac_object[0]):
                yield stac_object

    @property
    def pruned_products(self) -> Iterator[pystac.STACObject]:
        for stac_object in self.removed_objects:
            if StacRepositoryExtension.implements(stac_object):
                yield stac_object
