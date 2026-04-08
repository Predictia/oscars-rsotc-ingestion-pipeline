import logging

from ingestion_pipeline.data.derived_indices.available_indices import (
    list_available_indices,
)
from ingestion_pipeline.data.derived_indices.indices.time_models import (
    AggregationType,
)
from ingestion_pipeline.derived_indices import DerivedIndicesPipeline

logger = logging.getLogger(__name__)


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s — %(name)s — %(levelname)s — %(message)s",
    )
    logger.info("Starting derived indices computation")
    for i in list_available_indices():
        w = DerivedIndicesPipeline(
            indice=i,
            temporal_aggregation=[
                AggregationType.MONTHLY,
                AggregationType.SEASONAL,
                AggregationType.ANNUAL,
            ],
            overwrite=True,
        )
        w.run_pipeline()
        logger.info(f"Process for {i} finished")

    logger.info("Process finished")


if __name__ == "__main__":
    main()
