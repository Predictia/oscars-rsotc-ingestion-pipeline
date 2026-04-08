from unittest.mock import MagicMock

import pandas as pd
import pytest
import xarray as xr

from ingestion_pipeline.data.derived_indices.indices.api import (
    _compute_direct_function,
    _get_main_variable_name,
)


class MockVar:
    def __init__(self, short_name):
        self.short_name = short_name


def test_get_main_variable_name():
    index = MagicMock()
    index.vars2use = [MockVar("var1"), MockVar("var2")]
    # is_multivariable is not used anymore in the simplified function but let's set it
    index.is_multivariable = True
    assert _get_main_variable_name(index) == "var1"


def test_compute_direct_function_catch_all():
    def mock_func(*args, **kwargs):
        return args[0]

    ds = xr.Dataset(
        {"var": (("time",), [1.0])}, coords={"time": [pd.Timestamp("2020-01-01")]}
    )

    # This should succeed
    _compute_direct_function(ds, ds, "MS", mock_func, {}, "single_dataset")

    # This should raise RuntimeError with the correct message
    with pytest.raises(RuntimeError, match="Unkown signature unknown"):
        _compute_direct_function(ds, ds, "MS", mock_func, {}, "unknown")
