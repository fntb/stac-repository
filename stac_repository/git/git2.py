from __future__ import annotations
from typing import (
    Optional,
    overload,
    Literal,
    BinaryIO,
    Union,
    cast,
    Dict
)
import os
import io
import subprocess
from typing import NamedTuple
import datetime
import re
from functools import cached_property
import urllib
import urllib.parse
import logging
import abc
import contextlib
import tempfile
import sys
import shutil

if sys.version_info >= (3, 9):
    from functools import cache
else:
    from functools import lru_cache

    def cache(user_function, /):
        return lru_cache(maxsize=None)(user_function)

from ..__about__ import __version__, __name_public__
from .cache import CacheMeta

_logger = logging.getLogger(f"{__name_public__}:git")


class GitError(Exception):

    @staticmethod
    def make(message: str, code: int = 1):
        if re.search(r"path.+does not exist", message) is not None:
            return FileNotFoundError(message)
        elif re.search(r"pathspec.+did not match any files", message) is not None:
            return FileNotFoundError(message)
        else:
            return GitError(message, code=code)

    _code: int

    @property
    def code(self):
        return self._code

    def __init__(self, *args: object, code: int = 1):
        super().__init__(*args)
        self._code = code


class RefNotFoundError(ValueError):
    pass


class SignatureError(ValueError):
    pass


class Signature(NamedTuple):

    @staticmethod
    @cache
    def make(signature_str: str) -> Signature:
        result = re.fullmatch("(.*)<(.*)>$", signature_str)

        if result is None:
            raise SignatureError(
                f"Invalid git signature string {signature_str}")

        name = result.group(1).strip()
        email = result.group(2).strip()

        return Signature(name=name, email=email)

    name: str
    email: str = ""

    def __str__(self) -> str:
        return f"{self.name} <{self.email}>"


@overload
def git(
    *args: str,
    text: Literal[True] = True,
    env: Optional[dict[str, str]] = None,
    cwd: Optional[str] = None,
    input: Optional[Union[str, bytes]] = None,
) -> str:
    ...


@overload
def git(
    *args: str,
    text: Literal[False],
    env: Optional[dict[str, str]] = None,
    cwd: Optional[str] = None,
    input: Optional[Union[str, bytes]] = None,
) -> bytes:
    ...


def git(
    *args: str,
    env: Optional[dict[str, str]] = None,
    cwd: Optional[str] = None,
    input: Optional[Union[str, bytes]] = None,
    text: bool = True
) -> Union[str, bytes]:

    result = subprocess.run(
        [
            "git",
            *args
        ],
        cwd=cwd,
        capture_output=True,
        text=text,
        env=env,
        input=input
    )

    if result.returncode != 0:
        if text:
            _logger.debug("\n" + result.stderr)
        raise GitError.make(result.stderr, code=result.returncode)

    if text:
        _logger.debug("\n" + result.stdout)

    return result.stdout


class Commit():

    ref: str
    _repository_dir: str

    def __init__(self, ref: str, *, repository_dir: str):
        self.ref = ref
        self._repository_dir = repository_dir

    @property
    def datetime(self):
        return datetime.datetime.fromtimestamp(
            float(git(
                "show",
                "-s",
                r"--format=%ct",
                self.ref,
                cwd=self._repository_dir
            ).strip()),
            datetime.timezone.utc
        )

    @property
    def message(self):
        return git(
            "show",
            "-s",
            r"--format=%B",
            self.ref,
            cwd=self._repository_dir
        ).strip()

    @property
    def parent(self):
        parent_ref = git(
            "rev-list",
            "-1",
            self.ref,
            cwd=self._repository_dir
        ).strip()

        if parent_ref:
            return Commit(ref=parent_ref, repository_dir=self._repository_dir)
        else:
            return None

    def lfs_smudge(self, pointer: str):
        return git(
            "lfs",
            "smudge",
            text=False,
            input=pointer.encode("utf-8"),
            cwd=self._repository_dir
        )

    @overload
    def read(self, file: str, text: Literal[True] = True) -> str:
        ...

    @overload
    def read(self, file: str, text: Literal[False]) -> bytes:
        ...

    def read(self, file: str, text: bool = True) -> str | bytes:
        file_name = os.path.relpath(file, self._repository_dir)

        return git(
            "show",
            f"{self.ref}:{file_name}",
            cwd=self._repository_dir,
            text=text
        )

    @property
    def modified_files(self):
        file_names = git(
            "show",
            "--format=",
            "--name-only",
            self.ref,
            cwd=self._repository_dir
        ).strip().splitlines()

        return [os.path.join(self._repository_dir, file_name) for file_name in file_names]


class RemoteRepository():

    _repository_url: str
    _lfs_url: Optional[str]

    def __init__(self, repository_url: str, lfs_url: Optional[str] = None) -> None:
        self._repository_url = repository_url
        self._lfs_url = lfs_url

        try:
            git("ls-remote", repository_url)
        except GitError as error:
            raise ValueError(f"{repository_url} is not a git repository")

    @contextlib.contextmanager
    def tempclone(self, dir: str = tempfile.mkdtemp(), env: Dict[str, str] = {}):

        try:
            git("clone", self._repository_url, dir, env=env)

            repository = LocalRepository(dir, lfs_url=self._lfs_url)

            yield repository

            repository.push()
        finally:
            shutil.rmtree(dir, ignore_errors=True)

    def clone(self, dir: str = tempfile.mkdtemp(), env: Dict[str, str] = {}):

        git("clone", self._repository_url, dir, env=env)

        return LocalRepository(dir, lfs_url=self._lfs_url)


class LocalRepository():

    _repository_dir: str

    def __init__(self, repository_dir: str, lfs_url: Optional[str] = None) -> None:
        self._repository_dir = repository_dir

        if lfs_url:
            git(
                "config",
                "lfs.url",
                lfs_url,
                cwd=repository_dir
            )
        else:
            git(
                "config",
                "--unset",
                "lfs.url",
                cwd=repository_dir
            )

    @property
    def is_lfs_installed(self) -> bool:
        try:
            git("lfs", "version", cwd=self._repository_dir)
        except GitError:
            return False
        else:
            return True

    def lfs_fetch(self, file: Optional[str] = None):
        git(
            "lfs",
            "fetch",
            "--all" if file is None else file
        )

    def lfs_track(self, file: str):
        git(
            "lfs",
            "track",
            os.path.relpath(file, self._repository_dir)
        )

    def lfs_smudge(self, pointer: str):
        return git(
            "lfs",
            "smudge",
            text=False,
            input=pointer.encode("utf-8"),
            cwd=self._repository_dir
        )

    def get_commit(self, ref: str) -> Optional[Commit]:
        try:
            result = git(
                "rev-parse",
                ref
            )

            return Commit(result, repository_dir=self._repository_dir)
        except GitError:
            return None

    def __getitem__(self, ref: str) -> Optional[Commit]:
        return self.get_commit(ref)

    @property
    def head(self) -> Optional[Commit]:
        return self.get_commit("HEAD")

    def add(self, *added_files: str):
        git(
            "add",
            *[os.path.relpath(modified_file, self._repository_dir) for modified_file in added_files],
            cwd=self._repository_dir
        )

    def remove(self, *removed_files: str):
        git(
            "rm",
            *[os.path.relpath(modified_file, self._repository_dir) for modified_file in removed_files],
            cwd=self._repository_dir
        )

    def stage_all(self):
        git(
            "add",
            "--all",
            cwd=self._repository_dir
        )

    def commit(
        self,
        message: str,
    ) -> Commit:

        signature = Signature.make(f"{__name_public__}:{__version__} <>")

        git(
            "commit",
            "--allow-empty",
            "-m",
            message,
            env={
                **dict(os.environ),
                "GIT_AUTHOR_NAME": signature.name,
                "GIT_AUTHOR_EMAIL": signature.email,
                "GIT_COMMITTER_NAME": signature.name,
                "GIT_COMMITTER_EMAIL": signature.email,
                "GIT_COMMITTER_DATE": datetime.datetime.now(datetime.timezone.utc).isoformat()
            },
            cwd=self._repository_dir
        )

        return cast(Commit, self.head)

    def pull(self):
        git("pull", cwd=self._repository_dir)

    def push(self):
        git("push", cwd=self._repository_dir)

    def reset(self, ref: str = "HEAD", clean_modified_files: bool = False):
        commit = self[ref]
        if commit is None:
            raise RefNotFoundError

        git(
            "reset",
            "--hard",
            commit.ref,
            cwd=self._repository_dir
        )

        if clean_modified_files:
            self.clean()

    def clean(self):
        git(
            "clean",
            "-f",
            "-d",
            cwd=self._repository_dir
        )

    @overload
    def read(self, file: str, text: Literal[True] = True) -> str:
        ...

    @overload
    def read(self, file: str, text: Literal[False]) -> bytes:
        ...

    def read(self, file: str, text: bool = True) -> str | bytes:
        file_name = os.path.relpath(file, self._repository_dir)

        return git(
            "show",
            f":{file_name}",
            cwd=self._repository_dir,
            text=text
        )

    @property
    def modified_files(self):

        file_names = git(
            "ls-files",
            "--others",
            "--exclude-standard",
            cwd=self._repository_dir
        ).strip().splitlines()

        return [os.path.join(self._repository_dir, file_name) for file_name in file_names]
