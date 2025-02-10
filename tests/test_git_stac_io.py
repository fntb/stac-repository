import timeit
import warnings

import pytest
import pystac

from stac_repository.git.git_stac_io import GitIndexStacIO, GitCommitStacIO, GitStacIOWriteAttemptError

from .conftest import StacRepositoryDescription


class TestGitStacIO:

    def test_read_from_index(self, stac_repository_description: StacRepositoryDescription):

        indexed_catalog: pystac.Catalog

        def read_from_index():
            nonlocal indexed_catalog

            indexed_catalog = pystac.Catalog.from_file(
                stac_repository_description.index.catalog.get_self_href(),
                stac_io=GitIndexStacIO(
                    repository=stac_repository_description.index.repository
                )
            )

        time = timeit.timeit(read_from_index, number=1)

        warnings.warn(f"read_from_index() time profiling : {time}s")

        assert indexed_catalog.to_dict(
            include_self_link=False
        ) == stac_repository_description.index.catalog.to_dict(
            include_self_link=False
        )

    def test_read_from_commit(self, stac_repository_description: StacRepositoryDescription):

        committed_catalog: pystac.Catalog

        def read_from_commit():
            nonlocal committed_catalog

            committed_catalog = pystac.Catalog.from_file(
                stac_repository_description.head.catalog.get_self_href(),
                stac_io=GitCommitStacIO(
                    commit=stac_repository_description.head.repository.head
                )
            )

        time = timeit.timeit(read_from_commit, number=1)

        warnings.warn(f"read_from_commit() time profiling : {time}s")

        assert committed_catalog.to_dict(
            include_self_link=False
        ) == stac_repository_description.head.catalog.to_dict(
            include_self_link=False
        )

    def test_raise_on_write(self, stac_repository_description: StacRepositoryDescription):

        committed_catalog = pystac.Catalog.from_file(
            stac_repository_description.head.catalog.get_self_href(),
            stac_io=GitCommitStacIO(
                commit=stac_repository_description.head.repository.head
            )
        )

        try:
            committed_catalog.save()
        except GitStacIOWriteAttemptError:
            pass
        else:
            raise AssertionError("Expected GitStacIOWriteAttemptError")

        indexed_catalog = pystac.Catalog.from_file(
            stac_repository_description.index.catalog.get_self_href(),
            stac_io=GitIndexStacIO(
                repository=stac_repository_description.index.repository
            )
        )

        try:
            indexed_catalog.save()
        except GitStacIOWriteAttemptError:
            pass
        else:
            raise AssertionError("Expected GitStacIOWriteAttemptError")
