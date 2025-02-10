
import pystac

from ..base_stac_transaction import BaseStacTransaction
from .stac_commit import StacCommit
from .stac_repository import StacRepository


class StacTransaction(StacCommit, BaseStacTransaction):

    def __init__(
        self,
        repository: StacRepository,
    ):
        BaseStacTransaction.__init__(self)
        StacCommit.__init__(self, repository.catalog_file)

    def stage(self, *modified_objects: str | pystac.STACObject):
        pass

    def abort_unstaged(self):
        raise NotImplementedError

    def abort(self):
        raise NotImplementedError

    def commitable(self):
        pass

    def commit(self, *, message: None = None):
        pass
