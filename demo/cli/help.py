from typer.testing import CliRunner

from stac_repository_cli import app


def help_demo(**kwargs):
    runner = CliRunner()

    command = [
        "--help"
    ]

    result = runner.invoke(
        app,
        command
    )

    if result.exit_code != 0:
        raise result.exception

    return (command, result.stdout)
