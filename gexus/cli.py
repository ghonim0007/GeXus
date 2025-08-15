"""Command line interface for GeXus."""

import click
from . import __version__


@click.command()
@click.version_option(version=__version__)
def main():
    """GeXus: Big geospatial data processing framework."""
    click.echo(f"GeXus v{__version__}")
    click.echo("Welcome to GeXus - Big Geospatial Data Framework!")


if __name__ == "__main__":
    main()