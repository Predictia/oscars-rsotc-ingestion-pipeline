import json
import os
from pathlib import Path

import click
from jinja2 import Environment, FileSystemLoader


@click.command()
@click.option(
    "--template",
    required=True,
    help=(
        "Location of the Jinja template containing the static information required "
        "to generate the provenance."
    ),
)
@click.option(
    "--to-file",
    required=True,
    help="Destination file to save the rendered template.",
)
def render_config(
    template: str,
    to_file: str,
):
    template_path = Path(template)
    env = Environment(loader=FileSystemLoader(template_path.parent))
    template = env.get_template(template_path.name)

    context = {
        "author_list": json.loads(os.getenv("AUTHOR_LIST", "[]")),
        "download_variable_list": json.loads(os.getenv("DOWNLOAD_VARIABLE_LIST", "[]")),
        "compute_indice_list": json.loads(os.getenv("COMPUTE_INDICE_LIST", "[]")),
        "nuts_levels": json.loads(os.getenv("NUTS_LEVELS", "-1")),
    }
    to_file_path = Path(to_file)
    to_file_path.parent.mkdir(parents=True, exist_ok=True)
    with to_file_path.open(mode="w", encoding="utf-8") as f:
        f.write(template.render(context))


if __name__ == "__main__":
    render_config()
