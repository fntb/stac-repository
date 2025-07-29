from __future__ import annotations

from typing import (
    Protocol,
    Any,
    Optional,
    Dict,
    Iterator,
    BinaryIO,
    cast
)

from enum import Flag

from contextlib import contextmanager

import os
from urllib.parse import (
    urlparse as _urlparse,
)

import orjson
import requests


class JSONObjectError(ValueError):
    """Object is not a valid JSON object."""
    pass


# class FileNotInRepositoryError(ValueError):
#     pass


class HrefError(ValueError):
    """Href cannot be processed by the current StacIO implementation."""
    pass


class ReadableStacIO(Protocol):

    def get(self, href: str) -> Any:
        """Reads a JSON object.

        This method is intended to retrieve STAC Objects, as such the returned 
        value should be a valid representation of a STAC object, this validation 
        is not handled by the ReadableStacIO but downstream however.

        Raises:
            HrefError:
            FileNotFoundError:
            JSONObjectError:
        """
        ...

    @contextmanager
    def get_asset(self, href: str) -> Iterator[BinaryIO]:
        """Reads a binary Object.

        This method is intended to retrieve Asset files.

        This method must implement the ContextManager Protocol :

        ```
        with stac_io.get_asset(href) as asset_stream:
            ...
        ```

        Raises:
            HrefError:
            FileNotFoundError:
        """
        ...


class StacIO(ReadableStacIO):

    def set(self, href: str, value: Any):
        """(Over)writes a JSON object, which is a valid representation of a STAC object.

        Raises:
            HrefError:
            JSONObjectError:
        """
        ...

    def set_asset(self, href: str, value: BinaryIO):
        """(Over)writes a binary Object.

        This method is intended to save Asset files.

        Raises:
            HrefError:
        """
        ...

    def unset(self, href: str):
        """Deletes whatever object (if it exists) at `href`.

        Raises:
            HrefError:
        """
        ...


class StacIOPerm(Flag):
    NONE = 0

    R_STAC = 1
    """Read STAC objects"""

    R_ASSETS = 2
    """Read STAC assets"""

    R_ANY = 1 | 2
    """Read STAC objects and assets"""

    RW_STAC = 1 | 4
    """Write (and read) STAC objects"""

    RW_ASSETS = 2 | 8
    """Write (and read) STAC assets"""

    RW_ANY = 1 | 2 | 4 | 8
    """Write (and read) STAC objects and assets"""


class DefaultReadableStacIO(ReadableStacIO):
    """A default implementation of `ReadableStacIO` operating on the local filesystem and over http(s)."""

    _perms: Dict[str, StacIOPerm]

    def __init__(self, perms: Dict[str, StacIOPerm] = {}) -> None:
        self._perms = perms

    def check_perms(self, href: str, required_perm: StacIOPerm) -> bool:
        for (base_href, perm) in self._perms.items():
            if required_perm in perm and href.startswith(base_href):
                return True

        return False

    @staticmethod
    def _is_file_href(href: str) -> bool:
        return _urlparse(href, scheme="").scheme == ""

    def get(self, href: str) -> Any:
        if not self.check_perms(href, StacIOPerm.R_STAC):
            raise HrefError(f"{href} is not within readable scope")

        href_scheme = _urlparse(href, scheme="").scheme

        if href_scheme == "":
            os_href = os.path.abspath(href)

            with open(os_href, "r+b") as object_stream:
                try:
                    return orjson.loads(object_stream.read())
                except orjson.JSONDecodeError as error:
                    raise JSONObjectError from error
        elif href_scheme in ["http", "https"]:
            response = requests.get(href)

            if response.status_code == 404:
                raise FileNotFoundError(f"{href} does not exist")
            else:
                response.raise_for_status()

            try:
                return response.json()
            except requests.JSONDecodeError as error:
                raise JSONObjectError from error
        else:
            raise HrefError(f"{href} cannot be fetched, it is neither a file nor a http(s) URI.")

    @contextmanager
    def get_asset(self, href: str) -> Iterator[BinaryIO]:
        if not self.check_perms(href, StacIOPerm.R_ASSETS):
            raise HrefError(f"{href} is not within readable assets scope")

        href_scheme = _urlparse(href, scheme="").scheme

        if href_scheme == "":
            os_href = os.path.abspath(href)

            with open(os_href, "r+b") as asset_stream:
                yield asset_stream
        elif href_scheme in ["http", "https"]:
            response = requests.get(href, stream=True)

            yield cast(BinaryIO, response.raw)
        else:
            raise HrefError(f"{href} cannot be fetched, it is neither a file nor a http(s) URI.")


class DefaultStacIO(DefaultReadableStacIO, StacIO):
    """A default implementation of `StacIO` operating on the local filesystem."""

    def set(self, href: str, value: Any):
        if not self.check_perms(href, StacIOPerm.RW_STAC):
            raise HrefError(f"{href} is not within writeable scope")

        if _urlparse(href, scheme="").scheme != "":
            raise HrefError(f"{href} cannot be set, it is not a file")

        os_href = os.path.abspath(href)

        os.makedirs(os.path.dirname(os_href), exist_ok=True)

        with open(os_href, "w+b") as object_stream:
            try:
                object_stream.write(orjson.dumps(value))
            except orjson.JSONEncodeError as error:
                raise JSONObjectError from error

    def set_asset(self, href: str, value: BinaryIO):
        if not self.check_perms(href, StacIOPerm.RW_ASSETS):
            raise HrefError(f"{href} is not within writeable assets scope")

        if _urlparse(href, scheme="").scheme != "":
            raise HrefError(f"{href} cannot be set, it is not a file")

        os_href = os.path.abspath(href)

        os.makedirs(os.path.dirname(os_href), exist_ok=True)

        with open(os_href, "w+b") as asset_stream:
            while (chunk := value.read()):
                asset_stream.write(chunk)

    def unset(self, href: str):
        if not self.check_perms(href, StacIOPerm.RW_ANY):
            raise HrefError(f"{href} is not within writeable scope")

        if _urlparse(href, scheme="").scheme != "":
            raise HrefError(f"{href} cannot be unset, it is not a file")

        os_href = os.path.abspath(href)

        try:
            os.remove(os_href)
        except FileNotFoundError:
            pass
