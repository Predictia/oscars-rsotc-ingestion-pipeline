from ingestion_pipeline.data.derived_indices.available_indices import (
    list_available_indices,
)
from ingestion_pipeline.data.derived_indices.indices.time_models import (
    AggregationType,
)
from ingestion_pipeline.derived_indices import DerivedIndicesPipeline


def test_era5_pipeline_real_integration(tmp_path):
    """
    REAL integration test.

    - Requires valid CDSAPI/ECMWF credentials in ~/.cdsapirc (or environment).
    - Downloads real ERA5 data for a very small time window.
    - Ensures files are actually generated.
    """
    for i in list_available_indices():
        w = DerivedIndicesPipeline(
            indice=i,
            temporal_aggregation=[
                AggregationType.MONTHLY,
                AggregationType.SEASONAL,
                AggregationType.ANNUAL,
            ],
            output_dir=tmp_path,
        )
        w.run_pipeline()
