from os import PathLike

from typer.testing import CliRunner

from stac_repository_cli import app

from ..generate_products import generate_demo_products
from .generate_config import generate_config


def ingest_products_demo(
        source: str = generate_demo_products(),
        config_file: PathLike[str] = generate_config()
):
    runner = CliRunner()

    command = [
        "ingest",
        "demo",
        source,
        "--config",
        config_file
    ]

    result = runner.invoke(
        app,
        command
    )

    if result.exit_code != 0:
        raise result.exception

    return (command, result.stdout)
