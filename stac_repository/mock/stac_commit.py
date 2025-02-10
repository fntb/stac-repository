
import hashlib
import pystac
import datetime

from ..base_stac_commit import BaseStacCommit


class StacCommit(BaseStacCommit):

    _catalog_file: str

    def __init__(self, root_catalog_file: str):
        self._catalog_file = root_catalog_file

    @property
    def id(self) -> str:
        return "working directory"

    @property
    def datetime(self) -> datetime.datetime:
        return datetime.datetime.now()

    @property
    def message(self) -> None:
        return None

    @property
    def parent(self) -> None:
        return None

    @property
    def catalog(self) -> pystac.Catalog:
        return pystac.Catalog.from_file(self._catalog_file)

    def fetch(self, href: str, *, text: bool = True, hash: bool = False) -> str | bytes:
        if hash:
            m = hashlib.sha256()
            with open(href, "rb") as pipe:
                while (data := pipe.read(65536)):
                    m.update(data)
            return m.hexdigest()
        else:
            with open(href, "r" if text else "rb") as pipe:
                return pipe.read()
