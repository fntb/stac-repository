
import contextlib
import abc

import pystac

from .base_stac_commit import BaseStacCommit


class BaseStacTransaction(BaseStacCommit, metaclass=abc.ABCMeta):

    @contextlib.contextmanager
    def context(self, *, message: str | None, **other_commit_args):

        try:
            yield self

            self.commit(
                **other_commit_args,
                message=message
            )
        except Exception as error:
            self.abort()
            raise error

    @abc.abstractmethod
    def stage(self, *modified_objects: str | pystac.STACObject):
        pass

    @abc.abstractmethod
    def abort_unstaged(self):
        raise NotImplementedError

    @abc.abstractmethod
    def abort(self):
        raise NotImplementedError

    @abc.abstractmethod
    def commitable(self):
        pass

    @abc.abstractmethod
    def commit(self, *, message: str | None):
        pass
