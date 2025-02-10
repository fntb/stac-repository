from tempfile import TemporaryDirectory

from demo import ingest_products_demo
from demo import cli_ingest_products_demo


def test_demo():

    with TemporaryDirectory() as dir:
        ingest_products_demo(dir=dir)


def test_cli_demo():

    with TemporaryDirectory() as dir:
        cli_ingest_products_demo()
