![OSCARS Logo](docs/_static/logo.png)

# Regional State of the Climate (RSOTC) Ingestion Pipeline

The Ingestion Pipeline is a core component of the **Regional State of the Climate (RSOTC)** project. It provides a FAIR-by-design workflow to download, process, and homogenize climate data from the Copernicus Climate Data Store (ERA5) for European political regions (NUTS).

## Project Overview

Human-made climate change is one of the main challenges current societies are facing. The RSOTC project aims to develop a web-based dashboard that provides maps and diagrams for European political regions, showing the evolution of climate indicators similar to those used in "State of the Climate" (SofC) reports.

## Technical Architecture

The ingestion pipeline is responsible for the "backend of the backend":

1. **Download:** Automatically fetches daily ERA5 data via the CDS API.
1. **Preprocessing:** Standardizes units, renames variables, and cleans metadata.
1. **Regional Aggregation:** Computes area-weighted averages for all NUTS levels (0, 1, 2, 3) using GeoJSON definitions.
1. **Zarr Conversion:** Stores both gridded and regional data in Zarr format for optimal access.
1. **Derived Indices:** Computes complex climate indices (e.g., heat stress indicators, precipitation extremes) from base variables.

## Data Definitions

For a detailed description of the structure, naming conventions, and specific variables produced by the pipeline, please refer to the [Data Dictionary](data_dictionary.md).

## Project Organization

```
├── ingestion_pipeline/    # Main package directory
│   ├── cli.py             # CLI entry point and command definitions
│   ├── ingestion.py       # Main ingestion logic
│   ├── derived_indices.py # Derived indices computation logic
│   ├── data/              # Data handling modules
│   │   ├── download/      # CDS API request generation
│   │   ├── preprocessing/ # Homogenization and standardization
│   │   └── derived_indices/ # Index definitions and algorithms
│   ├── resources/         # NUTS GeoJSON definitions
│   └── utilities/         # S3, Zarr, and technical utils
├── tests/                 # Unit and integration tests
├── scripts/               # Helper scripts
├── docs/                  # Documentation
├── pixi.toml              # Pixi configuration and tasks
└── pyproject.toml         # Project metadata and configuration
```

## Installation and Setup

This project uses [Pixi](https://pixi.sh) for dependency management and development tasks.

### 1. Install Pixi

If you don't have Pixi installed, follow the [official installation guide](https://pixi.sh/latest/#installation):

```bash
curl -fsSL https://pixi.sh/install.sh | bash
```

### 2. Set up the Environment

Clone the repository and install the desired environment:

- **Core (Production):** `pixi install` (installs only base dependencies)
- **Development:** `pixi install -e dev` (adds tests, linting, and dev tools)
- **Documentation:** `pixi install -e docs` (adds Sphinx and documentation tools)

### 3. Usage with Pixi

Commands should be run using `pixi run`. By default, it uses the `default` environment unless specified otherwise. For example:

```bash
pixi run ingestion-pipeline --help
```

To run a command in a specific environment:

```bash
pixi run -e dev unit-tests
```

## Useful Commands

The `pixi.toml` file defines several tasks for common development workflows:

| Task | Command | Environment | Description |
|---|---|---|---|
| `qa` | `pixi run qa` | `dev` | Run pre-commit hooks (linting, formatting) on all files. |
| `unit-tests` | `pixi run unit-tests` | `dev` | Run unit tests with coverage report. |
| `type-check` | `pixi run type-check` | `dev` | Run `mypy` for static type checking. |
| `docs-build` | `pixi run docs-build` | `docs` | Build the Sphinx documentation. |
| `docker-build` | `pixi run docker-build` | `default` | Build the project Docker image. |
| `docker-run` | `pixi run docker-run` | `default` | Run the Docker container. |
| `template-update` | `pixi run template-update` | `dev` | Update the project using `cruft`. |
| `bumpversion-*` | `pixi run bumpversion-patch` | `dev` | Bump the version (patch, minor, or major). |

### Versioning and Commits

This project uses [Commitizen](https://commitizen-tools.github.io/commitizen/) for automated versioning and to enforce [Conventional Commits](https://www.conventionalcommits.org/).

> \[!NOTE\]
> Commitizen and related tools are available in the **`dev` environment**. Ensure you have installed it with `pixi install -e dev`.

#### 1. Making Commits

To ensure your commit messages follow the conventional format, use:

```bash
pixi run -e dev cz commit
```

This will open an interactive prompt to help you construct a valid commit message.

#### 2. Upgrading the Version

To upgrade the version, you can use the following `pixi` tasks:

- **Patch:** `pixi run -e dev bumpversion-patch` (e.g., 1.0.0 -> 1.0.1)
- **Minor:** `pixi run -e dev bumpversion-minor` (e.g., 1.0.0 -> 1.1.0)
- **Major:** `pixi run -e dev bumpversion-major` (e.g., 1.0.0 -> 2.0.0)

These commands will:

1. Automatically update the version in `pyproject.toml`.
1. Create a git commit with the new version.
1. Create a git tag.

## Usage

### Download Command

Download historical data for a specific variable and area.

```bash
pixi run ingestion-pipeline download \
    --dataset "derived-era5-single-levels-daily-statistics" \
    --variable "2m_temperature_100" \
    --area "50;-10;30;20" \
    --start-date "2020-01-01" \
    --end-date "2022-12-31" \
    --max-workers 4 \
    --saving-temporal-aggregation "daily" \
    --saving-main-dir "/path/to/data" \
    --saving-chunks-size "500;50;50" \
    --overwrite
```

### Compute Derived Indices Command

Compute specialized indices from pre-downloaded ERA5 data.

```bash
pixi run ingestion-pipeline compute_derived_indices \
    --indice "tx35" \
    --temporal-aggregation "monthly" \
    --temporal-aggregation "annual" \
    --overwrite
```

### Provenance

Get a detailed provenance crate from the complete ingestion pipeline execution (`download` + `computed_derived_indices`). The output crate can be ava

```bash
pixi run ingestion-pipeline generate-crate \
    --workflow-spec <path-to-the-argo-specification>.yaml \
    --static-metadata-file <path-to-the-static-metadata-info>.yaml \
    --output-crate-zip <path-to-store-the-crate>.zip \
    --rocrate-profile <rocrate-profile> \
    --rocrate-gen-preview  <True|False> \ # Generate a HTML preview file for the crate
    --output-crate-path <path-for-rocrate-folder> \
    --output-crate-zip <path-for-rocrate-zip> \
    --pattern <pattern>
```

where:

- `<path-to-the-argo-specification.yaml>` must adhere to [Argo Workflow templates](https://argo-workflows.readthedocs.io/en/latest/workflow-templates/).
- `<path-to-the-yaml-static-metadata-info>` adheres to the format shown in the [sample configuration](./ingestion_pipeline/provenance/config/provenance_metadata_static_info.yaml.sample).
- `<rocrate-profile>` allows to select among the two RO-Crate profiles to generate the provenance of the workflow run, either `workflow-run-crate-0.5` or `provenance-run-crate-0.5`. The identifier complies with the [rocrate-validate profiles command](https://github.com/crs4/rocrate-validator).
- `<pattern>` is the pattern to match S3 objects for publishing (default: `"*_NUTS*.zarr"`).

#### Argo server credentials

In order to generate the retrospective provenance, this module gets workflow execution data from the Argo server API. For this to work, several pieces of information are required in order to access to the Argo API (which usually requires authentication), and the specific details of the workflow run, in particular the identifier and namespace where it has been executed. All the Argo-related information is currently is provided through a dotenv approach (`.env` file by default):

```
ARGO_SERVER="<argo-server-url>"
ARGO_NAMESPACE="<workflow-namespace>"
ARGO_TOKEN="<token>"
ARGO_SECURE=true
ARGO_INSECURE_SKIP_VERIFY=false
ARGO_WORKFLOW="<workflow-name>"
```

### Publication

Publish dataset results (data and provenance metadata) to Zenodo (requires `generate-crate`).

```bash
pixi run ingestion-pipeline publish-to-zenodo \
    --provenance-crate-zip <path-to-the-crate>.zip \
    --zenodo-token <token>
```

where:

- `path-to-the-crate>.zip` points to the provenance crate in zip format resulting from the [`generate-crate` command above](#provenance).

## License

Copyright 2024, European Union.
Licensed under the Apache License, Version 2.0. See the LICENSE file for details.
