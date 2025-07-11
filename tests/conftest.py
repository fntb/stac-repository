
from __future__ import annotations
import tempfile
import shutil
import random
from os import path
from os import PathLike
import os
import uuid
import string
from functools import cache
import tempfile

import pytest

import pystac
import pystac.layout

from stac_repository.git.git import Repository as GitRepository
from stac_repository.git.git import Signature as GitSignature
from stac_repository.git.git import Commit as GitCommit


@pytest.fixture
def make_dir():
    temp_dirs = []

    def _make_dir():
        temp_dir = tempfile.mkdtemp()
        temp_dirs.append(temp_dir)
        return temp_dir

    yield _make_dir

    for temp_dir in temp_dirs:
        shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture
def dir(make_dir):
    return make_dir()


@pytest.fixture
def make_file(dir):
    files = []

    # def _make_file(custom_dir: str | None = None, name: str | None = None, unnamed_ext: str = "txt", content: str = "Hello, World!"):
    def _make_file(custom_dir: str | None = None):
        content = "".join(random.choices(
            string.ascii_lowercase, k=random.randrange(2048, 4096, 1)))

        file = path.join(
            custom_dir or dir,
            uuid.uuid4().hex + ".txt"
        )

        with open(file, "w") as fd:
            fd.write("".join(
                random.choices(
                    string.ascii_lowercase,
                    k=random.randrange(2048, 4096, 1)
                )
            ))

        files.append(file)
        return file

    yield _make_file

    for file in files:
        try:
            os.remove(file)
        except FileNotFoundError:
            pass


@pytest.fixture
def file(make_file):
    return make_file()


@pytest.fixture
def make_large_file(dir):
    files = []

    # def _make_large_file(custom_dir: str | None = None, name: str | None = None, unnamed_ext: str = "bin",  mb_size: int | None = None):
    def _make_large_file(custom_dir: str | None = None):
        file = path.join(
            custom_dir or dir,
            uuid.uuid4().hex + ".bin"
        )

        with open(file, "wb") as fd:
            fd.write(os.urandom(random.randint(1, 10)*1_000_000))

        files.append(file)
        return file

    yield _make_large_file

    for file in files:
        try:
            os.remove(file)
        except FileNotFoundError:
            pass


@pytest.fixture
def large_file(make_large_file):
    return make_large_file()


@pytest.fixture
def make_files_sample(make_file, make_large_file):

    def _make_files_sample(dir: str | None = None):

        n_files = 4
        n_large_files = 2

        files = [make_file(dir) for _ in range(n_files)]
        large_files = [make_large_file(
            dir) for _ in range(n_large_files)]

        return files + large_files

    return _make_files_sample


@pytest.fixture
def files_sample(make_files_sample):
    return make_files_sample()


def overwrite_file(file: PathLike[str]):
    try:
        with open(file, "r", encoding="utf-8") as file_pipe:
            file_pipe.readlines()
    except UnicodeDecodeError:
        with open(file, "wb") as file_pipe:
            file_pipe.write(os.urandom(random.randint(1, 10)*1_000_000))
    else:
        with open(file, "w") as fd:
            fd.write("".join(
                random.choices(
                    string.ascii_lowercase,
                    k=random.randrange(2048, 4096, 1)
                )
            ))


class GitCommitDescription():
    repository: GitRepository

    added_files: list[str]
    removed_files: list[str]

    message: str
    signature: GitSignature
    tags: list[str]

    is_commited: bool = False

    def __init__(
        self,
        repository: GitRepository,
        added_files: list[str] = [],
        removed_files: list[str] = [],
        message: str = "Test",
        signature: GitSignature = GitSignature(name="test"),
        tags: list[str] = [],
    ):
        self.repository = repository
        self.added_files = added_files
        self.removed_files = removed_files
        self.message = message
        self.signature = signature
        self.tags = tags

    @cache
    def commit(self):
        if self.removed_files:
            self.repository.remove(*self.removed_files)

        if self.added_files:
            self.repository.add(*self.added_files)

        commit = self.repository.commit(
            self.message,
        )

        if self.tags:
            for tag in self.tags:
                commit.tag(tag)

        self.is_commited = True

        return commit


@pytest.fixture
def empty_repository(dir) -> GitRepository:
    repository = GitRepository(dir)
    repository.init()
    return repository


@pytest.fixture
def single_commit_repository(empty_repository: GitRepository, make_files_sample) -> GitCommitDescription:

    files = make_files_sample(empty_repository.dir)

    context = GitCommitDescription(
        repository=empty_repository,
        added_files=files,
        message="Initial commit"
    )

    context.commit()

    return context


@pytest.fixture
def multi_commit_repository(empty_repository: GitRepository, make_files_sample) -> list[GitCommitDescription]:

    log = []
    wt_files = []

    for n_commit in range(5):

        added_files = make_files_sample(empty_repository.dir)

        if len(wt_files > 1):
            removed_files = list(
                set(random.choices(wt_files, k=random.randrange(1, len(wt_files) - 1))))
        else:
            removed_files = []

        for file in removed_files:
            os.remove(file)

        if len(set(wt_files) - set(removed_files)) > 0:
            modified_files = list(set(random.choices(set(
                wt_files) - set(removed_files), k=random.randrange(1, len(set(wt_files) - set(removed_files))))))
        else:
            modified_files = []

        for file in modified_files:
            overwrite_file(file)

        context = GitCommitDescription(
            repository=empty_repository,
            added_files=added_files + modified_files,
            removed_files=removed_files,
            message=f"Commit {n_commit + 1}",
            tags=[f"commit-{n_commit + 1}"]
        )

        context.commit()
        log.append(context)

        wt_files = list(set(wt_files) + set(added_files) - set(removed_files))

    return log


class StacCommitDescription():
    repository: GitRepository

    catalog: pystac.Catalog

    object_ids: set[str]
    added_object_ids: set[str]
    removed_object_ids: set[str]
    modified_object_ids: set[str]

    product_ids: set[str]
    ingested_product_ids: set[str]
    reprocessed_product_ids: set[str]
    pruned_product_ids: set[str]

    is_staged: bool = False
    is_commited: bool = False

    _commit: GitCommit

    def __init__(
        self,
        repository: GitRepository,
        catalog: pystac.Catalog,
        object_ids: set[str],
        added_object_ids: set[str],
        removed_object_ids: set[str],
        modified_object_ids: set[str],
        product_ids: set[str],
        ingested_product_ids: set[str],
        reprocessed_product_ids: set[str],
        pruned_product_ids: set[str],
    ):
        self.repository = repository
        self.catalog = catalog

        self.object_ids = object_ids
        self.added_object_ids = added_object_ids
        self.modified_object_ids = modified_object_ids
        self.removed_object_ids = removed_object_ids

        self.product_ids = product_ids
        self.ingested_product_ids = ingested_product_ids
        self.reprocessed_product_ids = reprocessed_product_ids
        self.pruned_product_ids = pruned_product_ids

    def stage(self):
        if not self.is_staged:
            self.repository.stage_all()
            self.is_staged = True

    def commit(self):
        if not self.is_commited:
            commit = self.repository.commit(
                "test",
            )
            self.is_commited = True
            self._commit = commit

            return commit
        else:
            return self._commit


class StacRepositoryDescription(list[StacCommitDescription]):

    @property
    def _head_index(self):
        for (i, commit_description) in enumerate(reversed(self), start=1):
            if commit_description.is_commited:
                return len(self) - i
        else:
            return -1

    @property
    def working_directory(self) -> StacCommitDescription:
        if len(self) > 0 and not self[-1].is_commited and not self[-1].is_staged:
            return self[-1]

    @property
    def index(self) -> StacCommitDescription:
        i = self._head_index

        if len(self) > i + 1 and self[i + 1].is_staged:
            return self[i + 1]
        else:
            return None

    @property
    def head(self) -> StacCommitDescription:
        i = self._head_index

        if i >= 0:
            return self[i]

    @property
    def commits(self) -> list[StacCommitDescription]:
        i = self._head_index

        if i >= 0:
            return self[:i + 1]
        else:
            return []
