from typing import Iterator

from ..stac_repository import StacRepository
from ..stac_repository import StacRepositoryConfig
from .stac_repository_managed_mixin import StacRepositoryManagedMixin
from .stac_commit import StacCommitManaged
from ..stac_commit import StacCommit

from .stac_transaction_ingest import StacIngestTransaction
from .stac_transaction_prune import StacPruneTransaction


class StacRepositoryManaged(StacRepositoryManagedMixin, StacRepository):

    def ingest_products(self, processor_id, *product_sources, transaction_type: StacIngestTransaction = StacIngestTransaction):
        return super().ingest_products(processor_id, *product_sources, transaction_type=StacIngestTransaction)

    def ingest(self, processor_id, source):
        return super().ingest(processor_id, source, transaction_type=StacIngestTransaction)

    def prune(self, *product_ids):
        return super().prune(*product_ids, transaction_type=StacPruneTransaction)

    def history(self, product_id: str | None = None) -> Iterator[StacCommitManaged]:
        """Inspect the database history"""

        for commit in StacRepository.history(self, product_id):
            if isinstance(commit, StacCommit):
                yield StacCommitManaged(commit._commit, commit._catalog_file)
            else:
                raise TypeError(commit)
