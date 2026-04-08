import logging
import shutil
import sys
import tempfile
from pathlib import Path
from typing import List

import click
import markdown

from ingestion_pipeline.publication.zenodo import ZenodoClient
from ingestion_pipeline.utilities.s3_handlers import S3Config, S3Handler
from ingestion_pipeline.utilities.zip_utils import zip_directory

logger = logging.getLogger(__name__)

DEFAULT_TITLE = "OSCARS - Regional State of the Climate: Data and Indices"

DEFAULT_DESCRIPTION = """
This dataset provides harmonised regional climate data and climate-extreme indices developed within the
framework of the **Regional State of the Climate (RSOTC)** project.
This work was supported by the European Union’s *Horizon Europe* research and innovation programme through the
**OSCARS (Open Science Cluster of EOSC Regional State of the Climate)** project, Grant Agreement No.
**101058571**.

The dataset uses the **ERA5 reanalysis** as its foundational data source. ERA5 is the fifth-generation
atmospheric reanalysis produced by the **European Centre for Medium-Range Weather Forecasts (ECMWF)** and
distributed via the **Copernicus Climate Change Service (C3S)**
(DOI: https://doi.org/10.24381/cds.4991cf48).

ERA5 provides hourly estimates of a wide range of atmospheric, land-surface, and sea-state variables at a
spatial resolution of **0.25° x 0.25°** (approximately 31 km), covering the period from **1940 to the
present**. These data form the basis for deriving the consistent and high-resolution regional climate
information presented here.

The dataset provides regional information spatially aggregated as time series over European regions defined by
the **Nomenclature of Territorial Units for Statistics (NUTS)** classification:

- **NUTS 0**: Country level
- **NUTS 1**: Major socio-economic regions
- **NUTS 2**: Basic regions for regional policies
- **NUTS 3**: Small regions for specific diagnoses

(Reference: Eurostat - NUTS classification)

The dataset is designed to support **climate monitoring**, **impact assessment**, **policy analysis**, and
**downstream climate services** across multiple administrative scales.
The dataset is updated monthly. Early each month, the previous month's data (with a 5-day delay for ERA5) is
downloaded via a comprehensive process to ensure it remains current, relevant, and accurate.

---

## Core variables

The core variables are derived from the state-of-the-art ERA5 reanalysis and represent fundamental quantities
for climate and meteorological applications:

- **Near-surface air temperature** (`tas`, `tasmin`, `tasmax`)
  Daily mean (`tas`), maximum (`tasmax`), and minimum (`tasmin`) air temperature at 2 m.

- **Total precipitation** (`pr`)
  Daily accumulated liquid and frozen precipitation, relevant for hydrology, drought, and flood analysis.

- **10-m wind speed** (`sfcWind`)
  Magnitude of the horizontal wind vector at 10 m, relevant for wind energy and atmospheric transport studies.

---

## Derived indices

In addition to the core variables, the dataset includes a comprehensive set of **derived climate indices**
commonly used in climate-impact and risk assessments. Indices are calculated for multiple temporal
aggregations (annual, seasonal, and monthly).

### Hot days

- **tx30**: Number of days with maximum temperature (`tasmax`) > 30 °C
- **tx35**: Number of days with maximum temperature (`tasmax`) > 35 °C
- **tx40**: Number of days with maximum temperature (`tasmax`) > 40 °C

Example: the `tx35` value for January 2020 represents the number of days in that month with `tasmax` exceeding
35 °C.

### Tropical nights

- **tr20**: Number of nights with minimum temperature (`tasmin`) > 20 °C
- **tr25**: Number of nights with minimum temperature (`tasmin`) > 25 °C

### Frost days

- **fd**: Number of frost days with minimum temperature (`tasmin`) < 0 °C

### Precipitation indicators

- **r1mm**: Number of wet days (`pr` ≥ 1 mm)
- **r20mm**: Number of very heavy precipitation days (`pr` ≥ 20 mm)
- **r95ptot**: Total precipitation from very wet days (`pr` > 95th percentile)

All indices follow **established climate-index definitions**, ensuring consistency and comparability across
regions and time periods.

---

## Data format

All datasets are stored as **Zarr** data stores and distributed as **compressed ZIP archives** for long-term
archival and Zenodo compatibility.

Filenames follow the convention:

```bash
{variable}_{pressure_level}_{dataset}_{region_set}.zarr.zip
```

For surface variables, the pressure level is specified as `None`.

---

## Example usage

The following Python example demonstrates how to open a core variable and a derived index using **xarray**:

```python
import xarray as xr

ds_tasmin = xr.open_dataset(
    "tasmin_None_ERA5_NUTS-3.zarr",
    engine="zarr"
)

ds_fd = xr.open_dataset(
    "fd_None_ERA5_NUTS-3.zarr",
    engine="zarr"
)

<xarray.Dataset>
Dimensions:  (region: 1345, time: 31412)
Coordinates:
  * region  (region) object 'CZ020' 'CZ031' ... 'NL327' 'NL328'
  * time    (time) datetime64[ns] 1940-01-01 ... 2025-12-31
Data variables:
    tasmin  (time, region) float64

<xarray.Dataset>
Dimensions:      (time: 1021, time_filter: 17, region: 1345)
Coordinates:
  * region       (region) object 'CZ020' ... 'NL328'
  * time         (time) datetime64[ns] 1940-01-01 ... 2025-01-01
  * time_filter  (time_filter) 'Annual' 'Apr' ... 'SepNov'
Data variables:
    fd           (time, time_filter, region) timedelta64[ns]
```

- Core variables: daily data with time and region dimensions
- Derived indices: aggregated data with an additional time_filter dimension indicating the aggregation period

## Data production and reproducibility

All data products are generated using the **open-source RSOTC Ingestion Pipeline**, which implements a fully
automated and reproducible workflow for data retrieval, preprocessing, aggregation, and index calculation.

The pipeline follows **FAIR principles (Findable, Accessible, Interoperable, Reusable)**, ensuring transparent
provenance, standardised metadata, and long-term usability.

## Intended use

This dataset is intended for **researchers, climate service providers, policymakers**, and other stakeholders
requiring consistent regional climate information across Europe. Typical use cases include:

- Trend analysis
- Regional climate diagnostics
- Climate-impact indicators
- Integration into dashboards and decision-support tools

Users are encouraged to cite this dataset when using it in scientific publications, reports, or operational
climate services.
"""


def get_html_description(body: str) -> str:
    """Convert Markdown description to HTML for Zenodo.

    Zenodo description supports HTML formatting (headings, lists, links, etc.),
    but does not interpret raw Markdown reliably.
    """
    md_text = body.strip()

    # Convert Markdown to HTML
    html = markdown.markdown(
        md_text,
        extensions=[
            "extra",  # tables, fenced code blocks, etc.
            "sane_lists",  # better list handling
        ],
        output_format="html",
    )

    # Zenodo accepts HTML in description fields (basic formatting)
    return html


def robust_rmtree(path: Path):
    """Robustly remove a directory tree, handling some OSErrors."""
    if path.exists():
        try:
            shutil.rmtree(path, ignore_errors=True)
            # If it still exists (ignore_errors=True might leave some things), try again or log
            if path.exists():
                logger.warning(f"Failed to fully remove {path}")
        except Exception as e:
            logger.error(f"Error removing {path}: {e}")


@click.command()
@click.option(
    "--zenodo-token",
    required=True,
    envvar="ZENODO_TOKEN",
    help="Zenodo API personal access token.",
)
@click.option(
    "--sandbox/--no-sandbox",
    default=True,
    help="Use Zenodo Sandbox environment.",
)
@click.option(
    "--pattern",
    default="*_NUTS*.zarr",
    help="Pattern to match S3 objects for publishing.",
)
@click.option(
    "--title",
    default=DEFAULT_TITLE,
    help="Title for the Zenodo deposition.",
)
@click.option(
    "--keyword",
    "-k",
    multiple=True,
    default=[
        "OSCARS",
        "OSCARS project",
        "FAIR data",
        "Open Science",
        "EOSC",
        "ENVRI",
        "Geophysics",
        "Climate information",
        "Europe",
    ],
    help="Keywords for the Zenodo deposition.",
)
@click.option(
    "--community",
    "-c",
    multiple=True,
    default=["oscars"],
    help="Communities for the Zenodo deposition.",
)
@click.option(
    "--license",
    default="Apache-2.0",
    help="License identifier (SLUG) for the deposition.",
)
@click.option(
    "--draft/--publish",
    default=False,
    help="Leave as draft instead of publishing.",
)
def publish_to_zenodo(
    zenodo_token: str,
    sandbox: bool,
    pattern: str,
    title: str,
    keyword: List[str],
    community: List[str],
    license: str,
    draft: bool,
):
    """Publish all matching S3 bucket outputs to a single Zenodo entry with proper versioning."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    description_html = get_html_description(DEFAULT_DESCRIPTION)
    communities = [{"identifier": c} for c in community]

    # Initialize S3 and Zenodo clients
    try:
        s3_config = S3Config.from_env()
        s3_handler = S3Handler(s3_config)
    except Exception as e:
        click.echo(f"Error initializing S3: {e}", err=True)
        sys.exit(1)

    zenodo_client = ZenodoClient(zenodo_token, sandbox=sandbox)

    # List files in S3
    files = s3_handler.list_files(pattern=pattern)
    if not files:
        click.echo(f"No files found matching pattern: {pattern}")
        return

    click.echo(f"Found {len(files)} files to sync.")

    # Metadata dictionary for create/update
    metadata = {
        "title": title,
        "upload_type": "dataset",
        "description": description_html,
        "creators": [
            {
                "name": "Predictia Intelligent Data Solutions SL (predictia@predictia.es)"
            },
            {"name": "Instituto de Física de Cantabria (IFCA) (ifca@ifca.unican.es)"},
        ],
        "keywords": list(keyword),
        "communities": communities,
        "license": license,
    }

    # Check if deposition already exists
    try:
        deposition = zenodo_client.get_deposition_by_title(title)
    except Exception as e:
        click.echo(f"Error checking Zenodo for existing deposition: {e}", err=True)
        sys.exit(1)

    if deposition:
        click.echo(f"Deposition found (ID: {deposition['id']}).")
        # If it's already published, we need to create a new version
        if deposition["submitted"]:
            click.echo("Creating new version draft...")
            new_version_draft = zenodo_client.new_version(deposition["id"])
            dep_id = new_version_draft["id"]

            # Delete old files in the draft if they exist
            click.echo("Cleaning up files from previous version in the new draft...")
            for file in new_version_draft.get("files", []):
                zenodo_client.delete_file(dep_id, file["id"])

            # Update metadata in the new version
            zenodo_client.update_deposition_metadata(dep_id, metadata)
        else:
            click.echo("Using existing unpublished draft...")
            dep_id = deposition["id"]
            # Update metadata if it's a draft
            zenodo_client.update_deposition_metadata(dep_id, metadata)
    else:
        click.echo("Creating new deposition...")
        deposition = zenodo_client.create_deposition(metadata)
        dep_id = deposition["id"]

    # Iterate over files and upload them to the SAME deposition
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)

        try:
            for s3_path in files:
                dataset_name = Path(s3_path).name
                local_dataset_path = tmpdir_path / dataset_name
                zip_path = tmpdir_path / f"{dataset_name}.zip"

                click.echo(f"Processing dataset: {dataset_name}")

                # Download from S3
                click.echo(f"  Downloading {s3_path}...")
                s3_handler.fs.get(
                    s3_path.replace("s3://", ""),
                    str(local_dataset_path),
                    recursive=True,
                )

                # Zip the directory
                click.echo(f"  Zipping {local_dataset_path}...")
                zip_directory(local_dataset_path, zip_path)

                # Upload zip file
                click.echo(
                    f"  Uploading {zip_path.name} to Zenodo (Deposition ID: {dep_id})..."
                )
                zenodo_client.upload_file(dep_id, zip_path)

                # Clean up local files for this dataset to save space
                robust_rmtree(local_dataset_path)
                if zip_path.exists():
                    zip_path.unlink()
        except KeyboardInterrupt:
            click.echo("\nInterrupted by user. Cleaning up and exiting...", err=True)
            sys.exit(1)
        except Exception as e:
            click.echo(f"\nError during processing: {e}", err=True)
            sys.exit(1)

    # Finalize and publish
    if draft:
        click.echo(f"Leaving deposition ID: {dep_id} as draft (draft=True).")
    else:
        try:
            click.echo(f"Publishing deposition ID: {dep_id}...")
            zenodo_client.publish_deposition(dep_id)
            click.echo(f"Successfully published all data to Zenodo entry: {title}")
        except Exception as e:
            click.echo(
                f"Error publishing deposition: {e}. You may need to publish it manually on the "
                "Zenodo web interface.",
                err=True,
            )


if __name__ == "__main__":
    publish_to_zenodo()
