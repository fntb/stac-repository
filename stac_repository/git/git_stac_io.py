import pystac
import pystac.stac_io

from .git import Repository, Commit


class GitStacIOWriteAttemptError(NotImplementedError):
    pass


class GitCommitStacIO(pystac.stac_io.DefaultStacIO):

    _commit: Commit

    def __init__(self, *args: pystac.Any, commit: Commit,  **kwargs: pystac.Any):
        super().__init__(*args, **kwargs)
        self._commit = commit

    def read_text_from_href(self, href: str) -> str:
        return self._commit.show(href)

    def write_text_to_href(self, href: str, txt: str) -> None:
        raise GitStacIOWriteAttemptError


class GitIndexStacIO(pystac.stac_io.DefaultStacIO):

    _repository: Repository

    def __init__(self, *args: pystac.Any, repository: Repository, **kwargs: pystac.Any):
        super().__init__(*args, **kwargs)
        self._repository = repository

    def read_text_from_href(self, href: str) -> str:
        return self._repository.show(href)

    def write_text_to_href(self, href: str, txt: str) -> None:
        raise GitStacIOWriteAttemptError
