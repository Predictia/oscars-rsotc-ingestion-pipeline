"""
Index definitions for derived indices calculations.

Defines custom indices and helper functions for computing climate indices
using xrindices and xclim libraries.
"""

from xclim.indices import frost_days, tn_days_above, tx_days_above, wetdays

from ingestion_pipeline.data.derived_indices.indices.data_models import (
    ConstantThreshold,
    Index,
    ReferencePercentile,
)
from ingestion_pipeline.data.derived_indices.indices.index_computation import (
    fraction_over_thresh_p,
)
from ingestion_pipeline.data.derived_indices.indices.time_models import (
    AggregationType,
    YearPeriodFlexible,
)
from ingestion_pipeline.data.derived_indices.indices.variable_registry import VarEnum
from ingestion_pipeline.data.derived_indices.utils.wrappers import (
    wrap_datarray_funct,
)

# Temperature indices
"""
Index: Tropical nights (Tn > 20°C).
"""
tr20 = Index(
    short_name="tr20",
    long_name="tr20",
    units="days",
    direct_function=wrap_datarray_funct(tn_days_above),
    direct_function_kwargs=dict(thresh="20 degC"),
    direct_function_signature="single_dataset",
    vars2use=[VarEnum.tasmin],
    valid_dest_freq=(
        AggregationType.MONTHLY.value,
        AggregationType.SEASONAL.value,
        AggregationType.ANNUAL.value,
    ),
)
"""
Index: Tropical nights (Tn > 25°C).
"""
tr25 = tr20.copy_and_replace(
    short_name="tr25",
    long_name="tr25",
    direct_function_kwargs=dict(thresh="25 degC"),
)
"""
Index: Hot days (Tx > 30°C).
"""
tx30 = Index(
    short_name="tx30",
    long_name="tx30",
    units="days",
    direct_function=wrap_datarray_funct(tx_days_above),
    direct_function_kwargs=dict(thresh="30.0 degC"),
    direct_function_signature="single_dataset",
    vars2use=[VarEnum.tasmax],
)
"""
Index: Hot days (Tx > 35°C).
"""
tx35 = tx30.copy_and_replace(
    short_name="tx35",
    long_name="tx35",
    direct_function_kwargs=dict(thresh="35.0 degC"),
)
"""
Index: Hot days (Tx > 40°C).
"""
tx40 = tx30.copy_and_replace(
    short_name="tx40",
    long_name="tx40",
    direct_function_kwargs=dict(thresh="40.0 degC"),
)

"""
Index: Frost days (Tn < 0°C).

Counts the number of days when daily minimum temperature is below 0°C.
Useful for tracking freezing conditions relevant to agriculture and infrastructure.
"""

fd = Index(
    short_name="fd",
    long_name="Frost days",
    units="days",
    threshold=ConstantThreshold(0, "lt"),
    direct_function=wrap_datarray_funct(frost_days),
    direct_function_kwargs=dict(thresh="0 degC"),
    direct_function_signature="single_dataset",
    vars2use=[VarEnum.tasmin],
)


# Precipitation Indices
"""
Index: Wet days with heavy precipitation (Pr > 20 mm).
"""
r20mm = Index(
    short_name="r20mm",
    long_name="r20mm",
    units="days",
    direct_function=wrap_datarray_funct(wetdays),
    direct_function_kwargs=dict(thresh="20 mm/day"),
    direct_function_signature="single_dataset",
    vars2use=[VarEnum.pr],
    valid_dest_freq=(
        AggregationType.MONTHLY.value,
        AggregationType.SEASONAL.value,
        AggregationType.ANNUAL.value,
    ),
)

"""
Index: Wet days with heavy precipitation (Pr > 1 mm).
"""
r1mm = r20mm.copy_and_replace(
    short_name="r1mm",
    long_name="r1mm",
    direct_function_kwargs=dict(thresh="1 mm/day"),
)

"""
Index: Fraction of total precipitation above 95th percentile (r95pTOT).

Represents the proportion of total annual precipitation occurring on days
when daily precipitation exceeds the 95th percentile of the reference period.
High values indicate that precipitation is concentrated in extreme events.

Note: If we use `kind="regular"` with a `valid_dest_freq` different from "ANNUAL",
the function will return a Dataset with a "month" or "season" dimension. This is due to
xrindices.percentile..get_pertentile() functionality.
"""
r95ptot = Index(
    short_name="r95ptot",
    long_name="r95ptot",
    units="1",
    direct_function=fraction_over_thresh_p,
    direct_function_kwargs=dict(thresh="1 mm/day"),
    direct_function_signature="percentile",
    reference_percentile=ReferencePercentile(
        value=95,
        reference_period=YearPeriodFlexible(None, None),
        operator="gt",
        kind="doy",
    ),
    vars2use=[VarEnum.pr],
    valid_dest_freq=(
        AggregationType.MONTHLY.value,
        AggregationType.SEASONAL.value,
        AggregationType.ANNUAL.value,
    ),
)
