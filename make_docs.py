
from demo.cli.generate_config import generate_config
from demo.generate_products import generate_demo_products

from demo.cli import cli_ingest_products_demo
from demo.cli import cli_help_demo
from demo.cli import cli_history_demo

from stac_repository.__about__ import __name_public__


def make_docs():
    source = generate_demo_products()
    config_file = generate_config()

    def render_command(command: list[str], result: str):
        return "```console\n{0}\n```\n\n```\n{1}```\n".format(
            __name_public__ + " " + " ".join(command),
            result
        )

    docs = "\n\n".join(
        [
            render_command(*command_demo(
                source=source,
                config_file=config_file
            ))
            for command_demo
            in [
                cli_help_demo,
                cli_ingest_products_demo,
                cli_history_demo
            ]
        ]
    )

    print(docs)


if __name__ == "__main__":
    make_docs()
