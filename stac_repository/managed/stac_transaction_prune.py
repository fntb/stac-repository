
from .stac_transaction import StacTransactionManaged
from .stac_transaction import StacTransactionCommitError


class StacPruneTransaction(StacTransactionManaged):

    def commitable(self):
        super().commitable()

        ingested_products = list(self.ingested_products)
        reprocessed_products = list(self.reprocessed_products)

        if ingested_products:
            raise StacTransactionCommitError(
                "Illegal ingestion",
                stac_objects=ingested_products
            )

        if reprocessed_products:
            raise StacTransactionCommitError(
                "Illegal reprocessing",
                stac_objects=reprocessed_products
            )
