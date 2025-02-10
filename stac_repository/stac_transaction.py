
from __future__ import annotations
from os import path
from os import PathLike
from itertools import chain
from typing import Any

import pystac
import pystac.link

from .lib.stac import get_stac_object
from .lib.stac import get_stac_object_line
from .lib.stac import get_stac_object_hrefs
from .lib.href_is_path import href_is_path

from .__about__ import __version__, __name_public__

from .base_stac_transaction import BaseStacTransaction
from .git.git import Signature, AbstractTagStrategy
from .stac_repository import StacRepository
from .stac_index import StacIndex


def href_is_in_directory(href: str, dir: PathLike[str]) -> bool:
    return href_is_path(href) and href.startswith(dir)


def get_stac_object_line_hrefs_in_directory(stac_object: pystac.STACObject, dir: PathLike[str]) -> set[PathLike[str]]:

    hrefs = set(get_stac_object_hrefs(stac_object))

    for related_stac_object in get_stac_object_line(stac_object):
        hrefs |= set(get_stac_object_hrefs(related_stac_object))

    return set(
        href
        for href
        in hrefs
        if href_is_in_directory(href, dir)
    )


class StacTransactionCommitError(Exception):

    files: list[str]

    def __init__(self, msg="", *args: object, files: list[str] = [], stac_objects: list[pystac.STACObject] = [], **kwargs) -> None:
        self.files = files
        self.stac_objects = stac_objects

        stac_objects = [
            str(stac_object)
            for stac_object
            in stac_objects
        ]

        self.add_note(f"{files=}")
        self.add_note(f"{stac_objects=}")

        super().__init__(msg, *args, **kwargs)


class StacTransactionStagingError(Exception):
    pass


class StacTransaction(StacIndex, BaseStacTransaction):

    _default_signature: Signature
    _context_commit_args: dict[str, Any]

    def __init__(
        self,
        repository: StacRepository,
    ):
        BaseStacTransaction.__init__(self)
        StacIndex.__init__(self, repository._repository,
                           repository.catalog_file)
        self._default_signature = repository.signature

    def stage(self, *modified_objects: str | pystac.STACObject):
        for modified_object in modified_objects:
            modified_object_id = modified_object.id if isinstance(
                modified_object, pystac.STACObject) else modified_object

            wt_object = get_stac_object(
                pystac.Catalog.from_file(self._catalog_file),
                modified_object_id
            )
            object = get_stac_object(self.catalog, modified_object_id)

            if wt_object is None and object is None:
                raise StacTransactionStagingError

            if object is not None:

                underlying_files = get_stac_object_line_hrefs_in_directory(
                    object, dir=self._repository.dir)

                for file in underlying_files:
                    if not path.lexists(file):
                        self._repository.remove(file)
                    else:
                        self._repository.add(file)

            if wt_object is not None:

                underlying_files = get_stac_object_line_hrefs_in_directory(
                    wt_object, dir=self._repository.dir)

                self._repository.add(*underlying_files)

    def abort_unstaged(self):
        self._repository.clean()

    def abort(self):
        self._repository.reset(clean_modified_files=True)

    def commitable(self):
        if self._repository.modified_files:
            raise StacTransactionCommitError(
                f"Unexpected unstaged files",
                files=self._repository.modified_files
            )

    def commit(
            self,
            *,
            message: str = "Transaction",
            signature: Signature | str | None = None,
            tag: str | AbstractTagStrategy | None = None
    ):
        if isinstance(signature, str):
            signature = Signature.make(signature)
        elif signature is None:
            signature = self._default_signature
        else:
            signature

        self.commitable()

        self._repository.commit(
            message,
            signature,
            signature
        )

        if isinstance(tag, str):
            self._repository.head.tag(tag)
        elif isinstance(tag, AbstractTagStrategy):
            self._repository.head.tag(tag.make(self._repository))
