
from __future__ import annotations
from functools import cached_property

from .stac_commit_managed_mixin import StacCommitManagedMixin
from ..stac_index import StacIndex
from ..stac_commit import StacCommit
from .stac_commit import StacCommitManaged


class StacIndexManaged(StacCommitManagedMixin, StacIndex):

    @cached_property
    def parent(self) -> StacCommitManaged | None:
        parent: StacCommit | None = super().parent

        if parent is None:
            return None
        else:
            return StacCommitManaged(parent._commit, parent._catalog_file)
