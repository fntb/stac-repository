
from .stac_transaction import StacTransactionManaged
from .stac_transaction import StacTransactionCommitError


class StacIngestTransaction(StacTransactionManaged):

    def commitable(self):
        super().commitable()

        pruned_products = list(self.pruned_products)
        reprocessed_products = list(self.reprocessed_products)

        if pruned_products:
            raise StacTransactionCommitError(
                "Illegal pruning",
                stac_objects=pruned_products
            )

        if reprocessed_products:
            raise StacTransactionCommitError(
                "Illegal reprocessing",
                stac_objects=reprocessed_products
            )
