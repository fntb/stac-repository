from os import path
from os import PathLike
import tempfile

from stac_repository.managed import StacRepositoryManaged

from .generate_products import generate_demo_products


def ingest_products_demo(
    source: str = generate_demo_products(),
    dir: PathLike[str] | None = tempfile.mkdtemp(
        prefix="stac_repository-demo-"
    )
):
    dir = path.abspath(dir)
    repository = StacRepositoryManaged(dir)

    # Ingesting

    for product_source in repository.discover("demo", source):
        print(f"{product_source=}")

        for report in repository.ingest_products("demo", product_source):
            print(f"{str(report.context)=} : {str(report.details)=}")

    # .. or, equivalently,

    # for report in repository.ingest("demo", source):
    #     print(f"{str(report.context)=} : {str(report.details)=}")
