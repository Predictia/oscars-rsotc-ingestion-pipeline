import numpy as np
import pandas as pd
import pytest
import xarray as xr

from ingestion_pipeline.utilities.temporal_agg import TemporalAggregation


@pytest.fixture
def dummy_timeseries():
    # Create daily data for 2 years (2020-2021)
    dates = pd.date_range("2020-01-01", "2021-12-31", freq="D")
    data = np.random.rand(len(dates))
    ds = xr.Dataset({"temp": (("time",), data)}, coords={"time": dates})
    return ds


def test_sel_time_filter(dummy_timeseries):
    # Select Jan-Feb
    agg = TemporalAggregation(
        dataset=dummy_timeseries,
        statistical="mean",
        time_filter="01-02",
        product_type="climatology",
        period_range="2020-2021",
    )
    filtered = agg.sel_time_filter()

    # Should contain only Jan and Feb data
    valid_months = [1, 2]
    months = pd.to_datetime(filtered.time.values).month
    assert np.all(np.isin(months, valid_months))
    # 2020 is leap year (29 days in Feb), 2021 is not
    # 2020: 31+29 = 60
    # 2021: 31+28 = 59
    assert len(filtered.time) == 119


def test_compute_climatology(dummy_timeseries):
    agg = TemporalAggregation(
        dataset=dummy_timeseries,
        statistical="mean",
        time_filter="01-02",
        product_type="climatology",
    )
    data_period, climatology = agg.compute()

    # Climatology should aggregate over time, resulting in single value
    # for spatial dims (if any) or scalar-like
    # But wait, the code says: time_agg_product = time_agg_product.mean("time")
    # if original data had only time dim, result has no time dim
    assert "time" not in climatology.dims
    assert climatology["temp"].ndim == 0


def test_compute_temporal_series(dummy_timeseries):
    agg = TemporalAggregation(
        dataset=dummy_timeseries,
        statistical="mean",
        time_filter="01-02",
        product_type="temporal_series",
    )
    _, time_series = agg.compute()

    # Logic: resample(time="YS").reduce("mean")
    # Should have one point per year
    assert "time" in time_series.dims
    assert len(time_series.time) == 2  # 2020, 2021
