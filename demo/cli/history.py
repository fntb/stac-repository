from os import PathLike

from typer.testing import CliRunner

from stac_repository_cli import app

from .generate_config import generate_config


def history_demo(
        config_file: PathLike[str] = generate_config(),
        **kwargs
):
    runner = CliRunner()

    command = [
        "history",
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
