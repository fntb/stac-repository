
import pytest

import pystac

from stac_repository.managed import BaseStacCommitManaged
from stac_repository.managed import StacCommitManaged
from stac_repository.managed import StacIndexManaged

from .conftest import StacCommitDescription
from .conftest import StacRepositoryDescription


class TestManagedStacCommit:

    @staticmethod
    def make_stac_commit(commit_description: StacCommitDescription) -> BaseStacCommitManaged:
        if commit_description.is_commited:
            return StacCommitManaged(
                commit_description.commit(),
                commit_description.catalog.get_self_href()
            )
        elif commit_description.is_staged:
            return StacIndexManaged(
                commit_description.repository,
                commit_description.catalog.get_self_href()
            )
        else:
            raise ValueError(commit_description)

    def test_parent_is_managed(self, stac_repository_description: StacRepositoryDescription):
        commit: StacIndexManaged = self.make_stac_commit(
            stac_repository_description.index
        )

        assert isinstance(commit.parent, StacCommitManaged)

        for i in range(1, len(stac_repository_description.commits)):
            commit: StacCommitManaged = self.make_stac_commit(
                stac_repository_description.commits[i]
            )

            assert isinstance(commit.parent, StacCommitManaged)

    def test_products(self, stac_repository_description: StacRepositoryDescription):
        def get_object_ids(*objects: pystac.STACObject | tuple[pystac.STACObject, pystac.STACObject]):
            return set(
                stac_object.id if isinstance(
                    stac_object, pystac.STACObject) else stac_object[0].id
                for stac_object
                in objects
            )

        def test_commit_objects(commit: BaseStacCommitManaged, commit_description: StacCommitDescription):
            assert get_object_ids(
                *commit.products
            ) == commit_description.product_ids

            assert get_object_ids(
                *commit.ingested_products
            ) == commit_description.ingested_product_ids

            assert get_object_ids(
                *commit.reprocessed_products
            ) == commit_description.reprocessed_product_ids

            assert get_object_ids(
                *commit.pruned_products
            ) == commit_description.pruned_product_ids

        commit: StacIndexManaged = self.make_stac_commit(
            stac_repository_description.index
        )

        test_commit_objects(commit, stac_repository_description.index)

        for i in range(len(stac_repository_description.commits)):
            commit: StacCommitManaged = self.make_stac_commit(
                stac_repository_description.commits[i]
            )

            test_commit_objects(commit, stac_repository_description.commits[i])
