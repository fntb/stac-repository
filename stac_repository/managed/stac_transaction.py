
from .stac_index import StacIndexManaged

from ..stac_repository import StacRepository
from ..stac_transaction import StacTransaction
from ..stac_transaction import StacTransactionCommitError
from ..stac_transaction import StacTransactionStagingError
from ..base_stac_transaction import BaseStacTransaction


class StacTransactionManaged(StacIndexManaged, StacTransaction):

    def __init__(
        self,
        repository: StacRepository,
    ):
        StacTransaction.__init__(self, repository)
        StacIndexManaged.__init__(
            self,
            repository._repository,
            repository.catalog_file
        )
