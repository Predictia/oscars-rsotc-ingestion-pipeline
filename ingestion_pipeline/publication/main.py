import logging
import sys
import zipfile
from pathlib import Path
from typing import List

import markdown

from ingestion_pipeline.publication.zenodo import ZenodoClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
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

All datasets are stored as **Zarr** data stores. Filenames follow the convention:

```bash
{variable}_{pressure_level}_{dataset}_{region_set}.zarr
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

The pipeline features the automatic recording of comprehensive metadata for the provenance of the individual
workflow runs packaged in accordance with the RO-Crate (Research Object Crate) specification. The content of
this dataset has been populated by means of this feature, by aggregating relevant (meta)data for the
provenance of the workflow execution, such as:
- `ro-crate-metadata.json`: contains metadata in JSON-LD format compliant with the Workflow Run RO-Crate
profile. Here, the metadata contains pointers to the main actors in the execution of the workflow including
the main workflow, steps, as well as input and output parameters (and values) within each step.
- `argo/workflow-template.yaml`: reproducible workflow specification used to generate the aforementioned data
for the [Core variables](#core-variables) and [Derived Indices](#derived-indices).
- Zarr files following the [data format already described](#data-format) resultant from the workflow
execution.

In line with the FAIR principles **(Findable, Accessible, Interoperable, Reusable)**, each pipeline execution
ensures traceability of the generated data products through comprehensive and transparent data provenance,
enabling higher standards of (re)usability and reproducibility.

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


class ZenodoPublisher:
    @staticmethod
    def run(
        provenance_crate_zip: str,
        zenodo_token: str,
        sandbox: bool,
        title: str,
        keyword: List[str],
        community: List[str],
        license: str,
        draft: bool,
        remove_zip: bool = False,
    ):
        """Publish all matching S3 bucket outputs to a single Zenodo entry with proper versioning."""
        description_html = get_html_description(DEFAULT_DESCRIPTION)
        communities = [{"identifier": c} for c in community]

        # Initialize Zenodo client
        zenodo_client = ZenodoClient(zenodo_token, sandbox=sandbox)

        # Metadata dictionary for create/update
        metadata = {
            "title": title,
            "upload_type": "dataset",
            "description": description_html,
            "creators": [
                {
                    "name": "Predictia Intelligent Data Solutions SL (predictia@predictia.es)"
                },
                {
                    "name": "Instituto de Física de Cantabria (IFCA) (ifca@ifca.unican.es)"
                },
            ],
            "keywords": list(keyword),
            "communities": communities,
            "license": license,
        }

        # Check if deposition already exists
        try:
            deposition = zenodo_client.get_deposition_by_title(title)
        except Exception as e:
            logger.debug(f"Error checking Zenodo for existing deposition: {e}")
            sys.exit(1)

        if deposition:
            logger.debug(f"Deposition found (ID: {deposition['id']}).")
            # If it's already published, we need to create a new version
            if deposition["submitted"]:
                logger.debug("Creating new version draft...")
                new_version_draft = zenodo_client.new_version(deposition["id"])
                dep_id = new_version_draft["id"]

                # Delete old files in the draft if they exist
                logger.debug(
                    "Cleaning up files from previous version in the new draft..."
                )
                for file in new_version_draft.get("files", []):
                    zenodo_client.delete_file(dep_id, file["id"])

                # Update metadata in the new version
                zenodo_client.update_deposition_metadata(dep_id, metadata)
            else:
                logger.debug("Using existing unpublished draft...")
                dep_id = deposition["id"]
                # Update metadata if it's a draft
                zenodo_client.update_deposition_metadata(dep_id, metadata)
        else:
            logger.debug("Creating new deposition...")
            deposition = zenodo_client.create_deposition(metadata)
            dep_id = deposition["id"]

        try:
            zip_path = Path(provenance_crate_zip)
            if not zipfile.is_zipfile(zip_path):
                raise ValueError(f"Provided crate is not in zip format: {zip_path}")

            # Upload zip file
            logger.debug(
                f"  Uploading {zip_path.name} to Zenodo (Deposition ID: {dep_id})..."
            )
            zenodo_client.upload_file(dep_id, zip_path)

            # Clean up local files to save space
            if remove_zip:
                if zip_path.exists():
                    zip_path.unlink()
        except KeyboardInterrupt:
            logger.debug("\nInterrupted by user. Cleaning up and exiting...")
            sys.exit(1)
        except Exception as e:
            logger.debug(f"\nError during processing: {e}")
            sys.exit(1)

        # Finalize and publish
        if draft:
            logger.debug(f"Leaving deposition ID: {dep_id} as draft (draft=True).")
        else:
            try:
                logger.debug(f"Publishing deposition ID: {dep_id}...")
                zenodo_client.publish_deposition(dep_id)
                logger.debug(
                    f"Successfully published all data to Zenodo entry: {title}"
                )
            except Exception as e:
                logger.debug(
                    f"Error publishing deposition: {e}. You may need to publish it manually on the "
                    "Zenodo web interface."
                )
