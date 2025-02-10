
from typing import Iterator, Never

from ...mock.stac_repository import StacRepository
from ...mock.stac_repository import StacRepositoryConfig
from ...mock.stac_commit import StacCommit
from ...mock.stac_transaction import StacTransaction
from ..stac_repository_managed_mixin import StacRepositoryManagedMixin
from .stac_commit import StacCommitManaged


class StacRepositoryManaged(StacRepositoryManagedMixin, StacRepository):

    def ingest_products(self, processor_id, *product_sources, transaction_type: StacTransaction = StacTransaction):
        return super().ingest_products(processor_id, *product_sources, transaction_type=StacTransaction)

    def ingest(self, processor_id, source):
        return super().ingest(processor_id, source, transaction_type=StacTransaction)

    def prune(self, *product_ids):
        return super().prune(*product_ids, transaction_type=StacTransaction)

    def history(self, product_id: str | None = None) -> Iterator[StacCommitManaged]:
        """Inspect the database history"""

        for commit in StacRepository.history(self, product_id):
            if isinstance(commit, StacCommit):
                yield StacCommitManaged(commit._catalog_file)
            else:
                raise TypeError(commit)
