

from __future__ import annotations
from functools import cached_property

from ..stac_commit_managed_mixin import StacCommitManagedMixin
from ...mock.stac_commit import StacCommit


class StacCommitManaged(StacCommitManagedMixin, StacCommit):

    @property
    def parent(self) -> StacCommitManaged | None:
        return None
