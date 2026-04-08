import dataclasses
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Literal

from ingestion_pipeline.data.derived_indices.indices.time_models import (
    VALID_FREQS,
    AggregationType,
    SeasonLimits,
    YearPeriod,
)
from ingestion_pipeline.data.derived_indices.indices.variable_registry import (
    VarEnum,
)


@dataclass
class ConstantThreshold:
    value: int | float
    operator: Literal["gt", "ge", "lt", "le"] = "ge"


@dataclass
class ReferencePercentile:
    """
    Defines the step of comparing against a reference percentile in compute_index.

    To be used when defining a climate index.

    Parameters
    ----------
    value : int
        Percentile to calculate, must be between 0 and 100.
    reference_period : YearPeriod
        Year interval to use as the reference period.
    operator : Literal["gt", "ge", "lt", "le"], optional
        Comparison operator to use agains the reference percentile. For example, "gt"
        is equivalent to dataset > reference_percentile. Default is "gt".
    kind : Literal["doy", "regular"], optional
        If "regular", compute_index will compute the percentile of the whole sample inside
        each time_filter specified by dest_freq. If "doy", it will use the percentile_doy
        xclim function to compute the percentile of a running window surrounding each day
        of year. The size of the window is 6 by default and can be set with the window
        parameter. Default is "regular".
    window : int, optional
        Window size if kind = "doy" is used. Default is 6.
    threshold : ConstantThreshold or None, optional
        If set, it will be used to filter out unwanted values before computing the
        percentile. It is mostly used for filter out zeroes for precipitation percentiles.
        Default is None.
    """

    value: int
    reference_period: YearPeriod
    operator: Literal["gt", "ge", "lt", "le"] = "gt"
    kind: Literal["doy", "regular"] = "regular"
    window: int = 6
    threshold: ConstantThreshold | None = None


@dataclass
class Index:
    """
    Index object that contains all details of a climate index definition.

    To be passed to compute_index together with the input data.

    Parameters
    ----------
    short_name : str
        Name to be used as key in the output Dataset when computing the index.
    long_name : str
        Long name to be used as key in the output Dataset when computing the index.
    units : str
        Units of the index. Use 1 for adimensional indices such as SPI.
    vars2use : list of VarEnum
        Variables needed as input to compute the index.
    direct_function : Callable or None, optional
        Function that fully computes the index. Default is None.
    direct_function_kwargs : dict, optional
        Extra arguments to be passed to direct_function, will be ignored is direct_function
        is None. Default is an empty dict.
    direct_function_signature : str, optional
        One of "historical", "percentile", "single_dataset". If "single_dataset" pass a
        single dataset, if historical pass also the historical
        and if "percentile" pass a reference percentile previously computed.
        Default is "historical".
    reference_percentile : ReferencePercentile or None, optional
        If defined, compute_index will calculate a reference percentile for the
        reference period, compare it with the input variable and pass the result to
        agg_function, that must be defined too. Default is None.
    threshold : ConstantThreshold or None, optional
        If defined, compute_index will, compare it with the input variable and pass the
        result to agg_function, that must be defined too. Default is None.
    season_limits : SeasonLimits or None, optional
        If defined, compute_index will filter the defined period inside each year. It is
        used mostly for agricultural indices such as Growing Season Length.
        Default is None.
    agg_function : Callable or None, optional
        Aggregation function to be used by resample. It must be defined unless
        direct_function is used. Default is None.
    valid_dest_freq : tuple of str, optional
        Use this to constrain some indices that must be only computed for the whole year,
        or only for monthly data. compute_index will raise an error if asked to use another
        destination frequency. Default is ("MS", "QS-DEC", "YS").
    """

    short_name: str
    long_name: str
    units: str
    vars2use: list[VarEnum]
    direct_function: Callable | None = None
    direct_function_kwargs: dict = field(default_factory=dict)
    direct_function_signature: str = "historical"
    reference_percentile: ReferencePercentile | None = None
    season_limits: SeasonLimits | None = None
    threshold: ConstantThreshold | None = None
    agg_function: Callable | None = None
    valid_dest_freq: tuple[str, ...] = VALID_FREQS

    @property
    def needs_reference_percentile(self) -> bool:
        """
        Check if reference percentile is needed.

        Returns
        -------
        bool
            True if reference percentile is needed.
        """
        return self.reference_percentile is not None

    @property
    def is_multivariable(self) -> bool:
        """
        Check if the index uses multiple variables.

        Returns
        -------
        bool
            True if multiple variables are used.
        """
        return len(self.vars2use) > 1

    def copy_and_replace(self, **kwargs) -> "Index":
        """
        Use to avoid repetition when two similar indices are to be defined.

        Parameters
        ----------
        **kwargs : Any
            Attributes to replace in the new copy.

        Returns
        -------
        Index
            A new Index instance with replaced attributes.
        """
        return dataclasses.replace(self, **kwargs)


@dataclass
class IndexConfig:
    """
    Configuration specification for a weather index.

    Encapsulates all metadata and computation parameters required
    to compute a specific weather index.

    Attributes
    ----------
    xindice_def : Index
        xrindices Index object defining the index computation.
    description : str
        Human-readable description of what the index represents.
    aggregations_allowed : list[AggregationType], optional
        Temporal aggregation types to compute. Defaults to
        [MONTHLY, SEASONAL, ANNUAL] if not provided.
    additional_params : dict[str, Any], optional
        Extra parameters for index computation (extensibility).

    Examples
    --------
    >>> config = IndexConfig(
    ...     xindice_def=tx35,
    ...     description="Days with Tmax > 35°C",
    ...     aggregations_allowed=[AggregationType.MONTHLY],
    ... )
    """

    xindice_def: Index
    description: str
    aggregations_allowed: list[AggregationType] = field(
        default_factory=lambda: [
            AggregationType.MONTHLY,
            AggregationType.SEASONAL,
            AggregationType.ANNUAL,
        ]
    )
    additional_params: dict[str, Any] = field(default_factory=dict)

    def __str__(self) -> str:
        """
        Return a string representation of the configuration.

        Returns
        -------
        str
            Human-readable description of the index and its aggregations.
        """
        agg_str = ", ".join(str(a) for a in self.aggregations_allowed)
        return f"IndexConfig({self.description} | Aggregations: {agg_str})"
