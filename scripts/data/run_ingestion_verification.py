"""
Script to verify the IngestionPipeline update functionality.

This script runs the IngestionPipeline twice for the 'tas' variable:
first for the year 2024 (initial write) and then for 2025 (incremental update).
"""

import logging
import os

from ingestion_pipeline.data.derived_indices.indices.time_models import (
    AggregationType,
)
from ingestion_pipeline.derived_indices import DerivedIndicesPipeline
from ingestion_pipeline.ingestion import IngestionPipeline


def run_ingestion_verification() -> None:
    """
    Run IngestionPipeline for 2024 and 2025 to verify S3 updates.

    This function initializes and executes the IngestionPipeline for the 2m temperature
    variable (tas) over two consecutive years. The first run creates the dataset
    on S3, while the second run tests the incremental update (append) logic
    using the 'can_be_updated' pattern.

    Returns
    -------
    None
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    logger = logging.getLogger(__name__)

    dataset_name = "derived-era5-single-levels-daily-statistics"
    variable_name = "tasmax_None"

    # Phase 1: Run for 2024
    logger.info("Starting Phase 1: Ingestion for 2024")
    pipeline_2024 = IngestionPipeline(
        dataset=dataset_name,
        variable=variable_name,
        start_date="2024-01-01",
        end_date="2024-12-31",
        area=[75, -30, 30, 50],
        max_workers=4,
        saving_main_directory=os.environ.get(
            "SAVING_MAIN_DIRECTORY", "data/oscars-rsotc/test"
        ),
        overwrite=False,
    )
    pipeline_2024.run_pipeline()

    pipeline_indices_2024 = DerivedIndicesPipeline(
        indice="tx35",
        temporal_aggregation=[
            AggregationType.MONTHLY,
            AggregationType.SEASONAL,
            AggregationType.ANNUAL,
        ],
        overwrite=True,
    )
    pipeline_indices_2024.run_pipeline()
    logger.info("Phase 1 completed.")

    # Phase 2: Run for 2025 (should trigger incremental update)
    logger.info("Starting Phase 2: Ingestion for 2025 (update)")
    pipeline_2025 = IngestionPipeline(
        dataset=dataset_name,
        variable=variable_name,
        start_date="2025-01-01",
        end_date="2025-12-31",
        area=[75, -30, 30, 50],
        max_workers=4,
        saving_main_directory=os.environ.get(
            "SAVING_MAIN_DIRECTORY", "data/oscars-rsotc/test"
        ),
        overwrite=False,
    )
    pipeline_2025.run_pipeline()

    pipeline_indices_2025 = DerivedIndicesPipeline(
        indice="tx35",
        temporal_aggregation=[
            AggregationType.MONTHLY,
            AggregationType.SEASONAL,
            AggregationType.ANNUAL,
        ],
        overwrite=False,
    )
    pipeline_indices_2025.run_pipeline()
    logger.info("Phase 2 completed. Verification script finished.")


if __name__ == "__main__":
    run_ingestion_verification()
