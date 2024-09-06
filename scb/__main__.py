"""Command-line interface."""
import click


@click.command()
@click.version_option()
def main() -> None:
    """SCB Python Wrapper."""


if __name__ == "__main__":
    main(prog_name="scb")  # pragma: no cover
