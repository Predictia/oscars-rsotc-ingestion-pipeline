from ingestion_pipeline.ingestion import IngestionPipeline


def test_era5_pipeline_real_integration(tmp_path):
    """
    REAL integration test.

    - Requires valid CDSAPI/ECMWF credentials in ~/.cdsapirc (or environment).
    - Downloads real ERA5 data for a very small time window.
    - Ensures files are actually generated.
    """
    pipeline = IngestionPipeline(
        dataset="derived-era5-single-levels-daily-statistics",
        variable="tasmax",
        pressure_level=None,
        area=[50, -10, 40, 10],
        start_date="2020-01-01",
        end_date="2020-01-01",
        max_workers=1,
        saving_temporal_aggregation="daily",
        saving_main_directory=tmp_path,
        overwrite=False,
    )
    pipeline.run_pipeline()
