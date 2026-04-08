"""
Registry of all available derived indices.

Contains index configurations and lookup functions.
"""

from ingestion_pipeline.data.derived_indices.indices.data_models import IndexConfig
from ingestion_pipeline.data.derived_indices.indices.index_definitions import (
    fd,
    r1mm,
    r20mm,
    r95ptot,
    tr20,
    tr25,
    tx30,
    tx35,
    tx40,
)

AVAILABLE_INDICES = {
    # Maximum Temperature Indices
    "tx30": IndexConfig(
        xindice_def=tx30,
        description="Number of days with maximum temperature above 30°C",
    ),
    "tx35": IndexConfig(
        xindice_def=tx35,
        description="Number of days with maximum temperature above 35°C",
    ),
    "tx40": IndexConfig(
        xindice_def=tx40,
        description="Number of days with maximum temperature above 40°C",
    ),
    # Minimum Temperature Indices
    "tr20": IndexConfig(
        xindice_def=tr20,
        description="Number of tropical nights with minimum temperature above 20°C",
    ),
    "tr25": IndexConfig(
        xindice_def=tr25,
        description="Number of equatorial nights with minimum temperature above 25°C",
    ),
    "fd": IndexConfig(
        xindice_def=fd,
        description="Number of frost days with minimum temperature below 0°C",
    ),
    # Precipitation Indices
    "r20mm": IndexConfig(
        xindice_def=r20mm,
        description="Number of days with precipitation above 20mm",
    ),
    "r1mm": IndexConfig(
        xindice_def=r1mm,
        description="Number of wet days with precipitation above 1mm",
    ),
    "r95ptot": IndexConfig(
        xindice_def=r95ptot,
        description="Fraction of total precipitation from days above 95th percentile",
    ),
}


def get_index_config(index_name: str) -> IndexConfig:
    """
    Retrieve configuration for a specific index.

    Parameters
    ----------
    index_name : str
        Name of the index (must be a key in AVAILABLE_INDICES).

    Returns
    -------
    IndexConfig
        Configuration object for the index.

    Raises
    ------
    KeyError
        If index_name is not in AVAILABLE_INDICES.
    ValueError
        If index_name is None or empty.

    Examples
    --------
    >>> config = get_index_config("tx35")
    >>> print(config.description)
    Number of days with maximum temperature above 35°C
    """
    if not index_name:
        raise ValueError("index_name cannot be None or empty")

    try:
        return AVAILABLE_INDICES[index_name]
    except KeyError:
        available = list(AVAILABLE_INDICES.keys())
        raise KeyError(
            f"Index '{index_name}' not found. Available indices: {available}"
        )


def list_available_indices() -> list[str]:
    """
    Get list of all available index names.

    Returns
    -------
    list[str]
        Sorted list of index names.
    """
    return sorted(AVAILABLE_INDICES.keys())
