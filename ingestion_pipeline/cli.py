import logging
import shutil
import sys
import tempfile
import warnings
from datetime import datetime
from pathlib import Path
from typing import List

import click

from ingestion_pipeline.data.derived_indices.indices.time_models import (
    AggregationType,
)
from ingestion_pipeline.derived_indices import DerivedIndicesPipeline
from ingestion_pipeline.ingestion import IngestionPipeline
from ingestion_pipeline.provenance.main import (
    ProvenanceTracker,
    ProvenanceTrackerStaticInfo,
)
from ingestion_pipeline.publication.main import DEFAULT_TITLE, ZenodoPublisher
from ingestion_pipeline.utilities.logging_config import setup_logging
from ingestion_pipeline.utilities.s3_handlers import S3Config, S3Handler

warnings.filterwarnings("ignore")

# Configure logging
setup_logging()

logger = logging.getLogger(__name__)


def validate_area(ctx, param, value):
    """Validate the area input to ensure it has 4 floating-point numbers."""
    if not value or (isinstance(value, str) and value.strip() == ""):
        return None
    try:
        area = [float(coord) for coord in value.split(";")]
        if len(area) != 4:
            raise ValueError
        return area
    except ValueError:
        raise click.BadParameter(
            "Area must be in the format 'north;west;south;east', with 4 float values."
        )


def validate_chunksize(ctx, param, value):
    """Validate the area input to ensure it has 4 floating-point numbers."""
    if not value or (isinstance(value, str) and value.strip() == ""):
        return None
    try:
        chunksize = [float(dim_size) for dim_size in value.split(";")]
        if len(chunksize) != 3:
            raise ValueError
        return {"time": chunksize[0], "lat": chunksize[1], "lon": chunksize[2]}
    except ValueError:
        raise click.BadParameter(
            "Chunk size must be in the format 'time;lat;lon', with 3 float values."
        )


def validate_date(ctx, param, value):
    """Validate date input and handle empty strings."""
    if not value or (isinstance(value, str) and value.strip() == ""):
        return None
    if isinstance(value, datetime):
        return value
    try:
        return datetime.strptime(value, "%Y-%m-%d")
    except ValueError:
        raise click.BadParameter("Format must be 'YYYY-MM-DD'.")


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


@click.group()
def cli():
    """CLI for managing ERA5 data downloads."""
    pass


@click.command()
@click.option(
    "--dataset",
    required=True,
    type=str,
    help="The dataset to download, e.g., 'derived-era5-single-levels-daily-statistics'.",
)
@click.option(
    "--variable",
    required=True,
    type=str,
    help="The variable to download, e.g., 't_100'.",
)
@click.option(
    "--area",
    required=True,
    callback=validate_area,
    help="The area to download as 'north;west;south;east', e.g., '50;-10;30;20'.",
)
@click.option(
    "--start-date",
    required=False,
    callback=validate_date,
    help="The start date for the data in 'YYYY-MM-DD' format.",
)
@click.option(
    "--end-date",
    required=False,
    callback=validate_date,
    help="The end date for the data in 'YYYY-MM-DD' format.",
)
@click.option(
    "--max-workers",
    default=1,
    type=int,
    show_default=True,
    help="The maximum number of workers for parallel downloading.",
)
@click.option(
    "--saving-temporal-aggregation",
    required=True,
    type=click.Choice(["daily", "monthly", "yearly"]),
    help="The temporal aggregation of the data to save (daily, monthly, or yearly).",
)
@click.option(
    "--saving-main-dir",
    required=True,
    type=click.Path(file_okay=False, writable=True, resolve_path=True),
    help="The main directory to save the downloaded data.",
)
@click.option(
    "--saving-chunks-size",
    callback=validate_chunksize,
    help="The chunk size for the data to save as 'time;lat;lon', e.g., '500;50;50'.",
)
@click.option(
    "--overwrite",
    is_flag=True,
    default=False,
    help="Overwrite existing files.",
)
def download(
    dataset,
    variable,
    area,
    start_date,
    end_date,
    max_workers,
    saving_temporal_aggregation,
    saving_main_dir,
    saving_chunks_size,
    overwrite,
):
    """Download historical data from the ERA5 dataset."""
    start_date_str = start_date.strftime("%Y-%m-%d") if start_date else None
    end_date_str = end_date.strftime("%Y-%m-%d") if end_date else None

    ingestion_pipe = IngestionPipeline(
        dataset=dataset,
        variable=variable,
        area=area,
        start_date=start_date_str,
        end_date=end_date_str,
        saving_temporal_aggregation=saving_temporal_aggregation,
        saving_main_directory=saving_main_dir,
        saving_chunks_size=saving_chunks_size,
        max_workers=max_workers,
        overwrite=overwrite,
    )

    if start_date and end_date:
        logger.info(
            f"Starting download from {start_date.date()} to {end_date.date()}..."
        )
    else:
        logger.info("Starting download with automatic date update...")

    ingestion_pipe.run_pipeline()
    logger.info("Download complete!")
    return "Success"


cli.add_command(download)


@click.command(name="compute_derived_indices")
@click.option(
    "--indice",
    required=True,
    type=str,
    help="The index to compute, e.g., 'tx35'.",
)
@click.option(
    "--temporal-aggregation",
    multiple=True,
    type=click.Choice(["daily", "weekly", "monthly", "seasonal", "annual"]),
    help="The temporal aggregation(s) to compute.",
)
@click.option(
    "--overwrite",
    is_flag=True,
    default=False,
    help="Overwrite existing files.",
)
def compute_derived_indices(
    indice,
    temporal_aggregation,
    overwrite,
):
    """Compute derived indices from ERA5 data."""
    # Map string choices to AggregationType
    agg_mapping = {
        "daily": AggregationType.DAILY,
        "weekly": AggregationType.WEEK,
        "monthly": AggregationType.MONTHLY,
        "seasonal": AggregationType.SEASONAL,
        "annual": AggregationType.ANNUAL,
    }

    # Convert tuple of strings to list of AggregationType
    # If no aggregation is provided, pass None to let DerivedIndices use defaults
    if not temporal_aggregation:
        aggregations = None
    else:
        aggregations = [agg_mapping[agg] for agg in temporal_aggregation]

    pipeline = DerivedIndicesPipeline(
        indice=indice,
        temporal_aggregation=aggregations,
        overwrite=overwrite,
    )

    logger.info(f"Computing index {indice}...")
    pipeline.run_pipeline()
    logger.info("Computation complete!")
    return "Success"


cli.add_command(compute_derived_indices)


@click.command(name="generate-crate")
@click.option(
    "--workflow-spec",
    type=str,
    required=True,
    help="Path to the workflow specification.",
)
@click.option(
    "--workflow-spec-ignore-step",
    type=str,
    multiple=True,
    required=False,
    default=("provenance",),
    help="Step name to ignore from the workflow specification.",
)
@click.option(
    "--static-metadata-file",
    type=str,
    required=False,
    help="Path to the configuration file for provenance generation.",
)
@click.option(
    "--rocrate-profile",
    type=click.Choice(["workflow-run-crate-0.5", "provenance-run-crate-0.5"]),
    required=False,
    default="workflow-run-crate-0.5",
    multiple=False,
    help=(
        "Identifier of the RO-Crate profile to be used to generate the provenance "
        "(valid identifier can be obtained with `rocrate-validate profiles` command)."
    ),
)
@click.option(
    "--rocrate-gen-preview",
    is_flag=True,
    default=False,
    required=False,
    help="Generate a HTML preview file for the crate (through ro-crate-py's `gen_preview` flag).",
)
@click.option(
    "--output-crate-path",
    type=str,
    required=False,
    help="Path to save the RO-Crate folder. Default is 'ingestion_pipeline_run_crate'.",
    default="ingestion_pipeline_run_crate",
)
@click.option(
    "--output-crate-zip",
    type=str,
    required=False,
    help="Path to save the RO-Crate in zip format.",
)
# FIXME to be obtained through 'artifacts' in YAML with static metadata
@click.option(
    "--pattern",
    default="*_NUTS*.zarr",
    help="Pattern to match S3 objects for publishing.",
)
def generate_crate(
    static_metadata_file,
    workflow_spec,
    workflow_spec_ignore_step,
    rocrate_profile,
    rocrate_gen_preview,
    output_crate_path,
    output_crate_zip,
    pattern,
):
    """Generate a RO-Crate for an ingestion pipeline run."""
    # Gather output files from S3 bucket
    try:
        s3_config = S3Config.from_env()
        s3_handler = S3Handler(s3_config)
    except Exception as e:
        click.echo(f"Error initializing S3: {e}", err=True)
        sys.exit(1)

    ## List files in S3
    files = s3_handler.list_files(pattern=pattern)
    if not files:
        click.echo(f"No files found matching pattern: {pattern}")
        return
    files_to_download = len(files)
    click.echo(f"Found {files_to_download} files to sync.")

    # Static metadata file
    static_metadata = ProvenanceTrackerStaticInfo(config_file=static_metadata_file)

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)
        try:
            downloaded_files = 1
            dataset_paths = {}
            for s3_path in files:
                dataset_name = Path(s3_path).name
                local_dataset_path = tmpdir_path / dataset_name

                # Download from S3
                s3_handler.fs.get(
                    s3_path.replace("s3://", ""),
                    str(local_dataset_path),
                    recursive=True,
                )
                click.echo(
                    f"  Downloaded {dataset_name} from {s3_path} to local path "
                    f"{local_dataset_path}... ({downloaded_files}/{files_to_download} )"
                )
                downloaded_files += 1
                dataset_paths[dataset_name] = {
                    "source_path": local_dataset_path.as_posix()
                }

        except KeyboardInterrupt:
            click.echo("\nInterrupted by user. Cleaning up and exiting...", err=True)
            sys.exit(1)
        except Exception as e:
            click.echo(f"\nError during processing: {e}", err=True)
            sys.exit(1)
        else:
            click.echo(
                f"All files processed successfully and available locally at {tmpdir_path}."
            )

        # Update dataset paths in static metadata (artifacts) to local temporary directory
        click.echo(f"Updating source paths for the output parameters: {dataset_paths}")
        static_metadata.output_paths = dataset_paths

        # Run provenance tracker
        provenance = ProvenanceTracker(
            static_metadata=static_metadata,
            workflow_spec=workflow_spec,
            workflow_spec_ignore_step=workflow_spec_ignore_step,
            rocrate_profile=rocrate_profile,
            rocrate_gen_preview=rocrate_gen_preview,
            output_crate_path=output_crate_path,
            output_crate_zip=output_crate_zip,
        )
        click.echo("Tracking provenance..")
        crate_path, crate_zip = provenance.run()
        if crate_path:
            click.echo(f"Provenance recording available under '{crate_path}' path")
        if crate_zip:
            click.echo(
                f"Provenance tracking in zip format available under: {crate_zip}"
            )


cli.add_command(generate_crate)


@click.command()
@click.option(
    "--provenance-crate-zip",
    required=True,
    help=(
        "Path to the zip file containting the RO-Crate's provenance (output of `generate-crate` "
        "command leveraging `--output-crate-zip` option)."
    ),
)
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
@click.option(
    "--remove-zip",
    default=False,
    help="Remove input zip file containing the crate.",
)
def publish_to_zenodo(
    provenance_crate_zip: str,
    zenodo_token: str,
    sandbox: bool,
    title: str,
    keyword: List[str],
    community: List[str],
    license: str,
    draft: bool,
    remove_zip: bool,
):
    """Publish provenance RO-Crate to Zenodo with proper versioning."""
    ZenodoPublisher.run(
        provenance_crate_zip=provenance_crate_zip,
        zenodo_token=zenodo_token,
        sandbox=sandbox,
        title=title,
        keyword=keyword,
        community=community,
        license=license,
        draft=draft,
        remove_zip=remove_zip,
    )


cli.add_command(publish_to_zenodo)
