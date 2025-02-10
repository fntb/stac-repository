

from __future__ import annotations
from functools import cached_property

from .stac_commit_managed_mixin import StacCommitManagedMixin
from ..stac_commit import StacCommit


class StacCommitManaged(StacCommitManagedMixin, StacCommit):

    @cached_property
    def parent(self) -> StacCommitManaged | None:
        parent: StacCommit | None = super().parent

        if parent is None:
            return None
        else:
            return StacCommitManaged(parent._commit, parent._catalog_file)

    @cached_property
    def products(self):
        return super().products

    @cached_property
    def ingested_products(self):
        return super().ingested_products

    @cached_property
    def reprocessed_products(self):
        return super().reprocessed_products

    @cached_property
    def pruned_products(self):
        return super().pruned_products
