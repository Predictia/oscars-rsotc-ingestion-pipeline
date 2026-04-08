import logging
from typing import List, Optional, Union

import pandas as pd
import xarray

from ingestion_pipeline.data.derived_indices.available_indices import (
    get_index_config,
)
from ingestion_pipeline.data.derived_indices.indices.api import compute_index
from ingestion_pipeline.data.derived_indices.indices.data_models import (
    IndexConfig,
)
from ingestion_pipeline.data.derived_indices.indices.time_models import (
    AggregationType,
)
from ingestion_pipeline.data.derived_indices.temporal_combiner import (
    DatasetCombiner,
    TimeReferenceCalculator,
)
from ingestion_pipeline.data.derived_indices.utils.time_utils import (
    filter_index_by_completeness,
    get_complete_period_timestamps,
)
from ingestion_pipeline.data.derived_indices.utils.units import (
    validate_and_fix_units,
)
from ingestion_pipeline.data.preprocessing.metadata_cleaning import (
    apply_dublin_core_metadata,
    update_dynamic_metadata,
)
from ingestion_pipeline.utilities.chunking import (
    chunk_dataset,
)
from ingestion_pipeline.utilities.constants import (
    REGION_SETS,
    REGION_ZARR_PATTERN,
    VARIABLE_ZARR_PATTERN,
)
from ingestion_pipeline.utilities.s3_handlers import (
    S3Config,
    S3Handler,
)
from ingestion_pipeline.utilities.spatial_agg import aggregate_regions

logger = logging.getLogger(__name__)


class DerivedIndicesPipeline:
    def __init__(
        self,
        indice: str,
        pressure_level: Optional[str] = None,
        source_paths: Optional[Union[List[str], str]] = None,
        output_dir: Optional[str] = None,
        temporal_aggregation: Optional[
            Union[List[AggregationType], AggregationType]
        ] = None,
        overwrite: bool = False,
    ) -> None:
        """
        Initialize DerivedIndicesPipeline instance.

        Parameters
        ----------
        indice : str
            The name of the derived index to compute.
        pressure_level : str | None, optional
            Pressure level for the index computation, by default None.
        source_paths : list[str] | str | None, optional
            Path(s) to the source data files, by default None.
        output_dir : str | None, optional
            Output directory for the computed indices, by default None.
        temporal_aggregation : list[AggregationType] | AggregationType | None, optional
            Temporal aggregation frequency or list of frequencies, by default None.
        overwrite : bool, optional
            If True, overwrite the files if they exist, by default False.
        """
        self.indice: IndexConfig = get_index_config(indice)
        self.pressure_level: Optional[str] = pressure_level
        self._source_paths_raw = source_paths
        self._output_dir_raw = output_dir
        self._temporal_aggregation_raw = temporal_aggregation
        self.source_paths: List[str] = []
        self.output_dir: str = ""
        self.temporal_aggregation: List[AggregationType] = []

        self.s3_handler = S3Handler(s3_config=S3Config.from_env())
        self.overwrite = overwrite
        self._validate_temp_aggregation()
        self._resolve_source_paths()
        self._resolve_output_dir()
        self.can_be_updated = self._check_can_be_updated()

    def _validate_temp_aggregation(self) -> None:
        """
        Ensure temporal aggregation is valid and normalized to a list.

        Raises
        ------
        ValueError
            If an aggregation frequency is not supported for the index.
        """
        # Validate aggregations
        if self._temporal_aggregation_raw is None:
            self.temporal_aggregation = [
                AggregationType(freq)
                for freq in self.indice.xindice_def.valid_dest_freq
            ]
            return
        elif isinstance(self._temporal_aggregation_raw, AggregationType):
            self.temporal_aggregation = [self._temporal_aggregation_raw]
        elif isinstance(self._temporal_aggregation_raw, list):
            # Assume it's a list of AggregationType
            self.temporal_aggregation = list(self._temporal_aggregation_raw)
        else:
            raise ValueError("Invalid temporal_aggregation type")

        for agg in self.temporal_aggregation:
            if agg.value not in self.indice.xindice_def.valid_dest_freq:
                raise ValueError(
                    f"Aggregation {agg} is not supported for index {self.indice}"
                )

    def _resolve_source_paths(self) -> None:
        """
        Resolve and validate source paths.

        This function applies the same file-naming pattern used in
        ingestion_pipeline/ingestion.py (ERA5 naming convention).
        """
        if self._source_paths_raw:
            paths = (
                [self._source_paths_raw]
                if isinstance(self._source_paths_raw, str)
                else list(self._source_paths_raw)
            )
            self.source_paths = [p for p in paths]
            return

        # Auto-generate source paths from required variables
        self.source_paths = self._build_default_source_paths()

    def _resolve_output_dir(self) -> None:
        """
        Resolve output directory.

        This function applies the same file-naming pattern used in
        ingestion_pipeline/ingestion.py (ERA5 naming convention).
        """
        if self._output_dir_raw:
            self.output_dir = self._output_dir_raw
            return

        # Default pattern for automatic generation
        self.output_dir = self.s3_handler.base_path

    def _build_default_source_paths(self) -> list[str]:
        """
        Build default source paths based on required variables.

        Returns
        -------
        list[str]
            List of S3 paths for required variables.

        Raises
        ------
        FileNotFoundError
            If any required source files are missing from S3.
        """
        expected_files = [
            VARIABLE_ZARR_PATTERN.format(
                variable=var.short_name, pressure_level=self.pressure_level
            )
            for var in self.indice.xindice_def.vars2use
        ]
        existing_files = [
            filename
            for filename in expected_files
            if self.s3_handler.file_exists(filename)
        ]
        if len(existing_files) != len(expected_files):
            missing_files = set(expected_files) - set(existing_files)
            raise FileNotFoundError(
                f"Missing required S3 files for index '{self.indice.xindice_def.short_name}': "
                f"{sorted(missing_files)}"
            )

        return [self.s3_handler.get_s3_path(file) for file in existing_files]

    def _generate_output_regions_path(self, region_set: str) -> str:
        """
        Build default output path for the computed index and region.

        Parameters
        ----------
        region_set : str
            The name of the region set (e.g., "NUTS0", "NUTS1").

        Returns
        -------
        str
            S3 path for the output zarr file.
        """
        filename = REGION_ZARR_PATTERN.format(
            variable=self.indice.xindice_def.short_name,
            pressure_level=self.pressure_level,
            region_set=region_set,
        )
        return f"{self.output_dir.rstrip('/')}/{filename}"

    def _check_can_be_updated(
        self,
    ) -> bool:
        """
        Check if all prerequisites exist for an incremental index update.

        Verifies that all regional files exist in storage and contain all required
        temporal aggregation frequencies. This ensures that the update can proceed
        incrementally from the last checkpoint without recomputing existing data.

        Returns
        -------
        bool
            True if all regional files exist and contain all required temporal
            aggregation frequencies, indicating the dataset is ready for an incremental
            update. False if any regional file is missing or any required frequency
            is not yet computed, meaning incremental updates cannot proceed.

        Notes
        -----
        This method assumes all regional files have consistent metadata (time coverage,
        frequencies_used attribute). If any prerequisite is missing, the entire pipeline
        should be rerun instead of attempting an incremental update.
        """
        for rs in REGION_SETS:
            output_path_rs = self._generate_output_regions_path(rs)
            if not self.s3_handler.path_exists(output_path_rs):
                return False

            attrs_zarr = self.s3_handler.inspect_zarr_metadata_in_s3(
                output_path_rs
            ).get("attrs", {})

            freqs = [
                f.strip() for f in attrs_zarr.get("frequencies_used", "").split(",")
            ]
            if not all(f.value in freqs for f in self.temporal_aggregation):
                return False
        return True

    def define_time_coverage(
        self, ds_vars: xarray.Dataset, aggregation: AggregationType
    ) -> xarray.Dataset | None:
        """
        Define the time coverage range for index calculation based on existing regional data.

        This method defines a time range for computing the index by checking the temporal
        coverage of existing regional datasets and their aggregation type. This approach
        allows computing indices for date ranges not yet present in regional datasets,
        avoiding unnecessary recalculation of temporal coverage.

        Parameters
        ----------
        ds_vars : xarray.Dataset
            Input dataset containing the source variables with a time coordinate.
        aggregation : AggregationType
            Aggregation type used to look up the checkpoint attribute
            (last_checkpoint_{aggregation.value}) in regional dataset metadata
            and to compute complete-period timestamps.

        Returns
        -------
        xarray.Dataset or None
            - Dataset with time dimension truncated to times >= the first complete-period
              timestamp after the stored checkpoint, if checkpoint exists and valid
              timestamps are found.
            - Original ds_vars unchanged if no checkpoint attribute exists in regional
              dataset metadata.
            - None if regional dataset is up-to-date and no truncation is required for that aggregation.
        """
        if self.overwrite or not self.can_be_updated:
            return ds_vars

        # Suppost that all regions have the same attributes and time coverage
        bucket, _ = self.s3_handler.split_s3_path(self.output_dir)

        region_attrs = self.s3_handler.inspect_zarr_metadata_in_s3(
            self.s3_handler.list_files(
                bucket=bucket,
                pattern=REGION_ZARR_PATTERN.format(
                    variable=self.indice.xindice_def.short_name,
                    pressure_level=self.pressure_level,
                    region_set="*",
                ),
            )[0],
        )

        # Select the last timestamp recorded for the aggregation
        attrs = region_attrs.get("attrs") if region_attrs else None
        if not attrs:
            return ds_vars
        ts_last_checkpoint = attrs.get(f"last_checkpoint_{aggregation.value}", None)
        if ts_last_checkpoint is None:
            return ds_vars
        ts_last_checkpoint = TimeReferenceCalculator.reverse_time_ref(
            time_ref=pd.Timestamp(ts_last_checkpoint),
            agg_type=aggregation,
        )

        # Check the possible timestamps can be created for the aggregation
        valid_timestamps = get_complete_period_timestamps(
            ds_vars[next(iter(ds_vars.data_vars))],
            aggregation,
        )

        # Check if the last checkpoint in regions is higher than any valid timestamp
        mask = valid_timestamps > ts_last_checkpoint
        closest_higher = valid_timestamps[mask].min() if mask.any() else None
        if closest_higher is None:
            return None

        return ds_vars.where(ds_vars.time >= closest_higher, drop=True)

    def run_pipeline(self) -> None:
        """
        Run the full derived indices computation pipeline.

        This includes:
        - Reading source data from S3.
        - Validating and fixing units.
        - Computing indices for each temporal aggregation.
        - Filtering by completeness.
        - Combining frequencies.
        - Aggregating by regions and uploading to S3.
        """
        # Read input data
        ds_list = [self.s3_handler.read_file(path) for path in self.source_paths]
        ds_vars = xarray.merge(ds_list)

        # Validate units and fix it for xclim package
        ds_vars = validate_and_fix_units(ds_vars, self.indice.xindice_def)

        # Compute index
        var_name = self.indice.xindice_def.short_name
        ds_index_dict: dict[AggregationType, xarray.Dataset] = {}
        logger.info(f"Computing index {self.indice.xindice_def}...")
        for t_agg in self.temporal_aggregation:
            ds_vars_aux = self.define_time_coverage(ds_vars, t_agg)
            if ds_vars_aux is None:
                continue

            ds_aux = compute_index(
                index=self.indice.xindice_def,
                input_dataset=ds_vars_aux,
                dest_freq=t_agg.value,
                orig_freq="D",
            )
            da_aux = filter_index_by_completeness(
                reference_da=ds_vars[self.indice.xindice_def.vars2use[0].short_name],
                target_da=ds_aux[var_name],
                aggregation=t_agg,
            )
            if da_aux.size == 0:
                continue
            ds_index_dict[t_agg] = da_aux.to_dataset(name=var_name)

        # Combine frequencies into a single dataset
        combiner = DatasetCombiner(self.indice.xindice_def)
        dataset = combiner.combine(datasets=ds_index_dict, multi_index=False)

        # Apply Dublin Core metadata to the gridded/combined dataset
        start_date = pd.to_datetime(dataset.time.values[0]).strftime("%Y-%m-%d")
        end_date = pd.to_datetime(dataset.time.values[-1]).strftime("%Y-%m-%d")

        dataset = apply_dublin_core_metadata(
            ds=dataset,
            variable_name=var_name,
            start_date=start_date,
            end_date=end_date,
            long_name=self.indice.xindice_def.long_name,
            description=self.indice.description,
            frequency=dataset.attrs.get("frequencies_used", "daily"),
        )
        # Add source paths to metadata
        dataset.attrs["dc:source"] += (
            f"; Derived from ERA5 files: {'; '.join([str(p) for p in self.source_paths])}"
        )

        # Regions upload
        for region_set in REGION_SETS:
            output_path = self._generate_output_regions_path(region_set)

            logger.info(f"Aggregating index to {region_set=}...")
            dataset_regions = aggregate_regions(dataset, region_set)  # type: ignore
            dataset_regions = chunk_dataset(ds=dataset_regions)

            # Apply Dublin Core metadata for regions
            dataset_regions = apply_dublin_core_metadata(
                ds=dataset_regions,
                variable_name=var_name,
                start_date=start_date,
                end_date=end_date,
                region_set=region_set,
                long_name=self.indice.xindice_def.long_name,
                description=self.indice.description,
                frequency=dataset_regions.attrs.get("frequencies_used", "daily"),
            )

            if self.can_be_updated:
                logger.info(f"Updating {self.indice.xindice_def.short_name} to S3")

                # Fetch existing metadata to preserve dc:created and start_date
                metadata = self.s3_handler.inspect_zarr_metadata_in_s3(output_path)
                old_attrs = metadata.get("attrs", {})
                valid_range = old_attrs.get("dcterms:valid", "")
                actual_start_date = (
                    valid_range.split("/")[0] if "/" in valid_range else start_date
                )

                dataset_regions = update_dynamic_metadata(
                    ds=dataset_regions,
                    variable_name=var_name,
                    start_date=actual_start_date,
                    end_date=end_date,
                    region_set=region_set,
                    long_name=self.indice.xindice_def.long_name,
                    description=self.indice.description,
                )
                # Preserve dc:created if possible
                if "dcterms:created" in old_attrs:
                    dataset_regions.attrs["dcterms:created"] = old_attrs[
                        "dcterms:created"
                    ]

                self.s3_handler.update_zarr_ds(
                    dataset=dataset_regions,
                    output_path=output_path,
                    append_dim="time",
                    attrs_to_update=[
                        att
                        for att in dataset_regions.attrs.keys()
                        if att.startswith("last_checkpoint_")
                        or att.startswith("dc")
                        or att.startswith("dcterms")
                    ],
                    reindex_dim="time_filter",
                    num_workers=4,
                )
            else:
                logger.info(f"Writing {self.indice.xindice_def.short_name} to S3")
                self.s3_handler.write_ds(
                    dataset=dataset_regions,
                    output_path=output_path,
                    overwrite=self.overwrite,
                    num_workers=4,
                )
