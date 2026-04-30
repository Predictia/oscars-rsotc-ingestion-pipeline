"""
ERA5 data ingestion and processing pipeline.

This module provides the IngestionPipeline class, which handles the end-to-end
process of downloading ERA5 data from CDS, homogenizing it, and storing it
as Zarr datasets on S3, including spatial aggregation by regions.
"""

import concurrent.futures
import logging
import os
import pathlib
import zipfile
from datetime import datetime, timedelta
from typing import List, Optional

import cdsapi
import pandas as pd
import xarray as xr
from dotenv import load_dotenv
from numcodecs import Blosc
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from ingestion_pipeline.data.download.generate_requests import generate_requests
from ingestion_pipeline.data.preprocessing.convert_units import convert_units
from ingestion_pipeline.data.preprocessing.ensure_relevant_info import (
    ensure_coordinate_existence,
)
from ingestion_pipeline.data.preprocessing.metadata_cleaning import (
    apply_dublin_core_metadata,
    remove_all_global_attrs,
    remove_all_var_and_coord_grib_attrs,
    update_dynamic_metadata,
)
from ingestion_pipeline.data.preprocessing.operations import ApplyTransformation
from ingestion_pipeline.data.preprocessing.renaming_dimensions import (
    rename_spatial_dimensions,
    rename_time_dimension,
)
from ingestion_pipeline.data.preprocessing.renaming_variables import (
    rename_variable_names,
)
from ingestion_pipeline.data.preprocessing.transforming_dimensions import (
    reindex_latitudes,
    reindex_longitudes,
    reindex_realization_number,
    reorder_dataset_dimensions,
)
from ingestion_pipeline.utilities.chunking import chunk_dataset
from ingestion_pipeline.utilities.constants import (
    REGION_SETS,
    REGION_ZARR_PATTERN,
    VARIABLE_ZARR_PATTERN,
)
from ingestion_pipeline.utilities.filename import generate_filename, parse_filename
from ingestion_pipeline.utilities.netcdf_utils import load_netcdf, write_netcdf
from ingestion_pipeline.utilities.s3_handlers import (
    S3Config,
    S3Handler,
)
from ingestion_pipeline.utilities.spatial_agg import aggregate_regions
from ingestion_pipeline.utilities.zip_utils import load_zip

load_dotenv()
CDS_API_KEY = os.getenv("CDS_API_KEY")


logger = logging.getLogger(__name__)


class IngestionPipelineError(Exception):
    """Custom exception for ingestion pipeline errors."""
    pass

class IngestionPipeline:
    """Manage downloading and processing of ERA5 data."""

    def __init__(
        self,
        dataset: str,
        variable: str,
        area: Optional[List[float]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        max_workers: int = 1,
        saving_temporal_aggregation: str = "monthly",
        saving_main_directory: Optional[str] = None,
        saving_chunks_size: Optional[dict] = None,
        overwrite: bool = False,
    ):
        """
        Initialize the ingestion pipeline with configuration and homogenization logic.

        Parameters
        ----------
        dataset : str, optional
            Dataset name (e.g., "reanalysis-era5-single-levels").
        variable : str, optional
            Variable to download defined as {variable_name}_{pressure_level}.
        area : list of float, optional
            Spatial subset [North, West, South, East].
        start_date : str, optional
            Start date for data downloads (YYYY-MM-DD). If None, defaults to "1940-01-01".
        end_date : str, optional
            End date for data downloads (YYYY-MM-DD). If None, defaults to the last day
            of the previous month.
        max_workers : int, optional
            Maximum number of threads for parallel downloads.
        saving_temporal_aggregation : str, optional
            Temporal aggregation (daily, monthly, or yearly).
        saving_main_directory : str, optional
            Directory to save downloaded and homogenized files. If None, a temporary
            directory is created.
        saving_chunks_size : dict, optional
            Dask chunking scheme. If None, uses default chunking logic.
        overwrite : bool, optional
            Whether to overwrite existing files on S3.
        """
        self.dataset = dataset
        self.variable = variable.split("_")[0]
        self.pressure_level = variable.split("_")[1]
        self.area = area or [90.0, -180.0, -90.0, 180.0]
        if start_date is None:
            self.start_date = "1940-01-01"
        else:
            self.start_date = start_date
        if end_date is None:
            today = datetime.utcnow().date()
            first_day_this_month = today.replace(day=1)
            last_day_prev_month = first_day_this_month - timedelta(days=1)
            self.end_date = last_day_prev_month.strftime("%Y-%m-%d")
        else:
            self.end_date = end_date
        self.max_workers = max_workers
        self.saving_temporal_aggregation = saving_temporal_aggregation
        if saving_main_directory is None:
            import tempfile

            self.saving_main_directory = tempfile.mkdtemp()
        else:
            self.saving_main_directory = saving_main_directory
        self.saving_chunks_size = saving_chunks_size
        self.client = cdsapi.Client(
            url="https://cds.climate.copernicus.eu/api", key=CDS_API_KEY, quiet=True
        )
        self.s3_handler = S3Handler(S3Config.from_env())
        self.overwrite = overwrite

    def _is_file_corrupted(self, file_path: str | pathlib.Path) -> bool:
        """
        Check if a NetCDF or ZIP file is corrupted by trying to open it.

        Parameters
        ----------
        file_path : str or pathlib.Path
            Path to the file to check.

        Returns
        -------
        bool
            True if the file is corrupted or cannot be opened, False otherwise.
        """
        path = pathlib.Path(file_path)
        if not path.exists():
            return False
        try:
            if path.suffix == ".nc":
                # open_dataset is lazy, so it's fast but checks basic structure
                with xr.open_dataset(path):
                    pass
            elif path.suffix == ".zip":
                with zipfile.ZipFile(path, "r") as zf:
                    # testzip() checks the CRC of each file in the archive
                    if zf.testzip() is not None:
                        return True
            return False
        except Exception:
            return True

    def _check_missing_or_corrupted_downloads(self, requests: List[dict]) -> List[dict]:
        """
        Check if any request has not been downloaded properly (file missing or corrupted).

        Parameters
        ----------
        requests : list of dict
            The list of download requests to check.

        Returns
        -------
        list of dict
            The list of requests that were not downloaded properly.
        """
        failed_requests = []
        for req in requests:
            file_path = req["file"]
            if not os.path.exists(file_path) or self._is_file_corrupted(file_path):
                failed_requests.append(req)
        return failed_requests

    def run_pipeline(self) -> tuple[xr.Dataset, dict[str, xr.Dataset]]:
        """
        Execute the download, homogenization, and aggregation pipeline.

        Returns
        -------
        tuple (xarray.Dataset, dict)
            A tuple containing:
            - dataset (xarray.Dataset): The gridded homogenized dataset.
            - datasets_regions (dict): A dictionary mapping region set names to their
              aggregated datasets.
        """
        # Gridded Processing
        zarr_key = VARIABLE_ZARR_PATTERN.format(
            variable=self.variable, pressure_level=self.pressure_level
        )
        zarr_path = self.s3_handler.get_s3_path(zarr_key)

        is_update = False
        if not self.overwrite and self.s3_handler.check_zarr_exists(zarr_key):
            logger.info(
                f"Zarr already exists: {zarr_key}. "
                "Checking start and end date to update if needed."
            )
            dataset_increment, dataset_full = self.update_gridded(zarr_key)
            if dataset_full is None:
                raise RuntimeError(f"Could not load or update dataset {zarr_key}")

            # Use the increment for following updates, or full if no increment was needed
            dataset = (
                dataset_increment if dataset_increment is not None else dataset_full
            )
            is_update = True
        else:
            downloaded_files = self.download()
            netcdf_files = [
                pathlib.Path(self.homogenize(pathlib.Path(file)))
                for file in downloaded_files
            ]
            dataset = xr.open_mfdataset(
                netcdf_files, combine="by_coords", parallel=True
            )
            dataset = chunk_dataset(ds=dataset, chunks=self.saving_chunks_size)

            # Prepare Dublin Core attributes
            dataset = apply_dublin_core_metadata(
                ds=dataset,
                variable_name=self.variable,
                start_date=self.start_date,
                end_date=self.end_date,
                area=self.area,
                frequency="daily",
            )

            encoding = {
                var: {
                    "compressor": Blosc(
                        cname="zstd", clevel=3, shuffle=Blosc.BITSHUFFLE
                    )
                }
                for var in dataset.data_vars
            }

            logger.info(f"Writing fresh gridded Zarr to {zarr_path}")
            self.s3_handler.write_ds(
                dataset,
                output_path=zarr_path,
                overwrite=self.overwrite,
                encoding=encoding,
            )

        # Regions Processing
        datasets_regions = {}

        for region_set in REGION_SETS:
            zarr_key_rs = REGION_ZARR_PATTERN.format(
                variable=self.variable,
                pressure_level=self.pressure_level,
                region_set=region_set,
            )
            zarr_path_rs = self.s3_handler.get_s3_path(zarr_key_rs)

            # Get the complete historical dataset for deriving any missing regional data
            source_ds_full = dataset_full if is_update else dataset

            if not self.overwrite and self.s3_handler.check_zarr_exists(zarr_key_rs):
                # Check the date range of the existing regional Zarr
                metadata_rs = self.s3_handler.inspect_zarr_metadata_in_s3(zarr_path_rs)
                attrs_rs = metadata_rs.get("attrs", {})
                valid_range_rs = attrs_rs.get("dcterms:valid")
                
                if valid_range_rs and "/" in valid_range_rs:
                    _, last_date_str_rs = valid_range_rs.split("/")
                    last_date_rs = pd.to_datetime(last_date_str_rs)
                else:
                    # Fallback to reading the dataset if attributes are missing
                    ds_rs = self.s3_handler.read_file(zarr_path_rs)
                    last_date_rs = pd.to_datetime(ds_rs.time.values[-1])
                    ds_rs.close()
                
                if last_date_rs >= pd.to_datetime(self.end_date):
                    logger.info(f"Regional dataset {zarr_key_rs} is already up to date. Skipping.")
                    continue
                
                increment_start = (last_date_rs + timedelta(days=1)).strftime("%Y-%m-%d")
                logger.info(f"Updating regional Zarr: {zarr_key_rs} from {increment_start} to {self.end_date}")
                
                # Slice the full dataset to get exactly the missing increment
                dataset_missing = source_ds_full.sel(time=slice(increment_start, self.end_date))
                
                if dataset_missing.time.size == 0:
                     logger.info(f"No new data to append for regional Zarr {zarr_key_rs}. Skipping.")
                     continue
                
                dataset_regions = self.update_regions(zarr_key_rs, dataset_missing, region_set)
                if dataset_regions is None:
                    raise RuntimeError(f"Could not update regions {zarr_key_rs}")
                datasets_regions[region_set] = dataset_regions
            else:
                # Creation or full overwrite
                logger.info(f"Writing fresh regional Zarr: {zarr_key_rs}")
                
                dataset_regions = aggregate_regions(source_ds_full, region_set)  # type: ignore
                dataset_regions.attrs["aggregation_method"] = (
                    f"Area-weighted averages for {region_set} regions "
                    "using GeoJSON definitions."
                )

                # Apply Dublin Core metadata for regions
                dataset_regions = apply_dublin_core_metadata(
                    ds=dataset_regions,
                    variable_name=self.variable,
                    start_date=source_ds_full.attrs.get("dcterms:valid", "").split("/")[0]
                    or self.start_date,
                    end_date=source_ds_full.attrs.get("dcterms:valid", "").split("/")[1]
                    or self.end_date,
                    region_set=region_set,
                    area=self.area,
                    frequency="daily",
                )

                dataset_regions = chunk_dataset(ds=dataset_regions)
                self.s3_handler.write_ds(
                    dataset_regions,
                    output_path=zarr_path_rs,
                    overwrite=self.overwrite,
                )
                datasets_regions[region_set] = dataset_regions
        for region_set, ds_reg in datasets_regions.items():
            ds_reg.close()
        if is_update:
            dataset_full.close()
            if dataset_increment is not None:
                dataset_increment.close()
        else:
            dataset.close()

        self.cleanup_directories()

        return dataset_full if is_update else dataset, datasets_regions

    def update_gridded(
        self, zarr_key: str
    ) -> tuple[Optional[xr.Dataset], Optional[xr.Dataset]]:
        """
        Update the gridded dataset on S3 with the latest available data.

        Parameters
        ----------
        zarr_key : str
            The S3 key (filename) of the gridded Zarr dataset to update.

        Returns
        -------
        tuple (incremental_ds, full_ds)
            incremental_ds : xarray.Dataset or None
                The new data added during this update. None if already up to date.
            full_ds : xarray.Dataset or None
                The complete dataset (all dates). Returns None if an error occurred.
        """
        # Try to get metadata from attributes first
        s3_path = self.s3_handler.get_s3_path(zarr_key)
        metadata = self.s3_handler.inspect_zarr_metadata_in_s3(s3_path)
        attrs = metadata.get("attrs", {})

        valid_range = attrs.get("dcterms:valid")
        if valid_range and "/" in valid_range:
            first_date_str, last_date_str = valid_range.split("/")
            last_date = pd.to_datetime(last_date_str)
            first_date_str = first_date_str.strip()
            logger.info(
                f"Existing dataset range from DC attributes: {first_date_str} to {last_date_str}"
            )
        else:
            # Fallback to reading the dataset if attributes are missing or legacy
            ds = self.s3_handler.read_file(s3_path)
            last_date = pd.to_datetime(ds.time.values[-1])
            first_date_str = pd.to_datetime(ds.time.values[0]).strftime("%Y-%m-%d")
            logger.info(f"Last date from data: {last_date}")

        if last_date >= pd.to_datetime(self.end_date):
            logger.info("Dataset is already up to date.")
            ds_full = self.s3_handler.read_file(s3_path)
            # Ensure full_ds has updated metadata if it was old
            if "dc:title" not in ds_full.attrs:
                logger.info("Updating legacy metadata to Dublin Core.")
                ds_full = apply_dublin_core_metadata(
                    ds_full,
                    self.variable,
                    first_date_str,
                    last_date.strftime("%Y-%m-%d"),
                    area=self.area,
                    frequency="daily",
                )
            return None, ds_full

        if first_date_str:
            logger.info(f"Existing dataset starts at: {first_date_str}")
            # We don't strictly assert self.start_date == first_date_str anymore
            # to allow incremental updates where self.start_date is the update start.

        new_start_date = (last_date + timedelta(days=1)).strftime("%Y-%m-%d")
        new_end_date = self.end_date

        logger.info(f"Updating dataset from {new_start_date} to {new_end_date}")

        # Update class attributes for the update period
        self.start_date = new_start_date
        self.end_date = new_end_date

        try:
            downloaded_files = self.download()
            if not downloaded_files:
                logger.info("No new data downloaded.")
                ds_full = self.s3_handler.read_file(s3_path)
                return None, ds_full

            netcdf_files = [
                self.homogenize(pathlib.Path(file)) for file in downloaded_files
            ]
            dataset = xr.open_mfdataset(
                netcdf_files, combine="by_coords", parallel=True
            )
            dataset = chunk_dataset(ds=dataset, chunks=self.saving_chunks_size)

            # Update dynamic attributes for the full range
            dataset = update_dynamic_metadata(
                ds=dataset,
                variable_name=self.variable,
                start_date=first_date_str,
                end_date=self.end_date,
                area=self.area,
            )

            # Append gridded data
            logger.info(f"Updating {self.variable} to S3")
            self.s3_handler.update_zarr_ds(
                dataset=dataset,
                output_path=s3_path,
                append_dim="time",
                num_workers=4,
            )

            # Load the full dataset to return
            ds_full = self.s3_handler.read_file(s3_path)
            return dataset, ds_full
        except Exception as e:
            logger.error(f"Failed to update gridded dataset: {e}")
            return None, None

    def update_regions(
        self, zarr_key: str, dataset: xr.Dataset, region_set: str
    ) -> Optional[xr.Dataset]:
        """
        Update a regional aggregation dataset on S3.

        Parameters
        ----------
        zarr_key : str
            The S3 key of the regional Zarr dataset to update.
        dataset : xarray.Dataset
            The gridded dataset to aggregate from.
        region_set : str
            The identifier for the region set (e.g., 'NUTS2').

        Returns
        -------
        xarray.Dataset or None
            The aggregated dataset if successful, or None otherwise.
        """
        try:
            dataset_regions = aggregate_regions(dataset, region_set)  # type: ignore
            dataset_regions.attrs["aggregation_method"] = (
                f"Area-weighted averages for {region_set} regions "
                "using GeoJSON definitions."
            )
            dataset_regions = chunk_dataset(
                ds=dataset_regions, chunks=self.saving_chunks_size
            )

            # Update dynamic attributes
            start_date_str = dataset_regions.attrs.get("dcterms:valid", "").split("/")[
                0
            ]
            dataset_regions = update_dynamic_metadata(
                ds=dataset_regions,
                variable_name=self.variable,
                start_date=start_date_str or self.start_date,
                end_date=self.end_date,
                region_set=region_set,
                area=self.area,
            )

            # Append regional data
            logger.info(f"Updating {self.variable} regional ({region_set}) to S3")
            self.s3_handler.update_zarr_ds(
                dataset=dataset_regions,
                output_path=self.s3_handler.get_s3_path(zarr_key),
                append_dim="time",
                attrs_to_update=[
                    att
                    for att in dataset_regions.attrs.keys()
                    if att.startswith("last_checkpoint_")
                ],
                num_workers=4,
            )
            return dataset_regions
        except Exception as e:
            logger.error(f"Error updating dataset regions: {e}")
            return None

    @retry(
        retry=retry_if_exception_type(IngestionPipelineError),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        stop=stop_after_attempt(3),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )
    def download(self) -> list[str]:
        """
        Download ERA5 data based on the pipeline configuration.

        This method generates download requests for the configured variables,
        area, and time range, and executes them in parallel using a thread pool.

        Returns
        -------
        list of str
            A list of local paths to the downloaded files.
        """
        logger.info("Generating download requests.")
        requests = generate_requests(
            self.dataset,
            self.variable,
            self.pressure_level,
            self.area,
            self.saving_temporal_aggregation,
            f"{self.saving_main_directory}/download",
            self.start_date,
            self.end_date,
        )

        @retry(
            retry=retry_if_exception_type(RuntimeError),
            wait=wait_exponential(multiplier=1, min=4, max=10),
            stop=stop_after_attempt(3),
            before_sleep=before_sleep_log(logger, logging.WARNING),
            reraise=True,
        )
        def download_data(request):
            file_name = request["file"]
            logger.debug(f"Processing request for {file_name}")
            if os.path.exists(file_name):
                if self._is_file_corrupted(file_name):
                    logger.warning(
                        f"File {file_name} already exists but is corrupted. Deleting."
                    )
                    os.remove(file_name)
                else:
                    logger.info(f"{file_name} already exists and is valid. Skipping.")
                    return file_name

            logger.info(f"Downloading {file_name}...")
            self.client.retrieve(
                request["catalogue_entry"], request["request"]
            ).download(file_name)
            logger.info(f"Downloaded {file_name}.")

            # Verify the download
            if self._is_file_corrupted(file_name):
                logger.warning(f"Downloaded file {file_name} is corrupted.")
                os.remove(file_name)
                raise RuntimeError(f"Downloaded file {file_name} is corrupted.")

            if not os.path.exists(file_name):
                raise RuntimeError(f"Downloaded file {file_name} does not exist.")

            return file_name

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=self.max_workers
        ) as executor:
            futures = [executor.submit(download_data, request) for request in requests]
            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"Error during parallel download attempt: {e}")

        # Check if all files are downloaded properly
        failed_requests = self._check_missing_or_corrupted_downloads(requests)
        if failed_requests:
            logger.warning(f"Total failed requests: {len(failed_requests)}")
            for req in failed_requests:
                file_path = req["file"]
                if os.path.exists(file_path):
                    logger.warning(f"Deleting failed/corrupted file: {file_path}")
                    os.remove(file_path)
            raise IngestionPipelineError(
                f"Some files (n={len(failed_requests)}) failed to download or are corrupted. "
                "Retrying the whole batch..."
            )

        return [req["file"] for req in requests]

    def homogenize(self, file_path: pathlib.Path):
        """
        Apply spatial, temporal, and unit homogenization to a downloaded file.

        Parameters
        ----------
        file_path : pathlib.Path
            Path to the downloaded file (NetCDF or ZIP).

        Returns
        -------
        str
            The path to the homogenized NetCDF file.

        Raises
        ------
        RuntimeError
            If the file type is unsupported.
        """
        (
            saving_directory,
            saving_temporal_aggregation,
            dataset,
            date,
            variable,
            pressure_level,
        ) = parse_filename(str(file_path))
        new_filename = generate_filename(
            f"{self.saving_main_directory}/homogenize",
            self.saving_temporal_aggregation,
            dataset,
            date,
            self.variable,
            self.pressure_level,
        )
        if pathlib.Path(new_filename).exists() and not self._is_file_corrupted(
            new_filename
        ):
            logger.info(f"File {new_filename} already exists. Skipping.")
        else:
            logger.info(f"Homogenizing the following file: {file_path}")
            if file_path.suffix == ".nc":
                ds = load_netcdf(file_path)
            elif file_path.suffix == ".zip":
                ds = load_zip(file_path)
            else:
                raise RuntimeError(f"Unsupported file type: {file_path}")
            if self.variable in ApplyTransformation.keys():
                ds = ApplyTransformation[self.variable](ds)
            ds = rename_variable_names(ds, self.variable)

            ds = remove_all_global_attrs(ds)
            ds = remove_all_var_and_coord_grib_attrs(ds)
            ds = ensure_coordinate_existence(ds)
            ds = rename_time_dimension(ds)
            ds = rename_spatial_dimensions(ds)
            ds = reindex_latitudes(ds)
            ds = reindex_longitudes(ds)
            ds = reindex_realization_number(ds)
            ds = reorder_dataset_dimensions(ds)
            ds = convert_units(ds)
            assert len(ds.data_vars) == 1
            new_filename = write_netcdf(ds, pathlib.Path(new_filename))
        return str(new_filename)

    def cleanup_directories(self) -> None:
        """
        Recursively remove all files and directories within the saving_main_directory.

        This method deletes all files in the directory tree rooted at
        `self.saving_main_directory` and then removes the directories themselves.
        """
        base_dir = pathlib.Path(self.saving_main_directory)
        logger.info(f"Starting cleanup of {base_dir}")

        if not base_dir.exists():
            logger.warning(f"Cleanup directory {base_dir} does not exist. Skipping.")
            return

        for root, dirs, files in os.walk(base_dir, topdown=False):
            # Remove all files
            for name in files:
                file_path = pathlib.Path(root) / name
                try:
                    file_path.unlink()
                    logger.debug(f"Deleted file: {file_path}")
                except Exception as e:
                    logger.error(f"Failed to delete file {file_path}: {e}")

            # Remove all directories
            for name in dirs:
                dir_path = pathlib.Path(root) / name
                try:
                    # Check if the directory is empty before removing
                    if not any(dir_path.iterdir()):
                        dir_path.rmdir()
                        logger.info(f"Removed empty directory: {dir_path}")
                    else:
                        logger.warning(
                            f"Directory not empty, skipping removal: {dir_path}"
                        )
                except Exception as e:
                    logger.error(f"Failed to remove directory {dir_path}: {e}")

        # Finally, try to remove the base directory itself if it's empty
        try:
            if base_dir.exists() and not any(base_dir.iterdir()):
                base_dir.rmdir()
                logger.info(f"Removed base directory: {base_dir}")
        except Exception as e:
            logger.error(f"Failed to remove base directory {base_dir}: {e}")

        logger.info("Cleanup completed.")
