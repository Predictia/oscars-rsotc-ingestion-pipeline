import logging

from ingestion_pipeline.ingestion import IngestionPipeline

logger = logging.getLogger(__name__)


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s — %(name)s — %(levelname)s — %(message)s",
    )
    logger.info("Starting ERA5 download pipeline")
    # Create an instance of the pipeline with the required parameters
    for variable in [
        "tas_None",
        "tasmax_None",
        "tasmin_None",
        "pr_None",
        "sfcWind_None",
    ]:
        pipeline = IngestionPipeline(
            dataset="derived-era5-single-levels-daily-statistics",
            variable=variable,
            area=[75, -30, 30, 50],
            saving_temporal_aggregation="monthly",
            saving_main_directory="/data/oscars-rsotc",
            max_workers=4,
            start_date=None,
            end_date=None,
            overwrite=False,
        )

        # Run the pipeline (downloads data and optionally homogenizes it)
        pipeline.run_pipeline()

        # Print the processed file paths
        logger.info(f"Process finished for {variable}")


if __name__ == "__main__":
    main()
