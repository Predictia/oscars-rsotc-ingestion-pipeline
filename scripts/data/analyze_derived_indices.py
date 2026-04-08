import logging
from typing import Any, Dict, Optional

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import xarray as xr
from plotly.subplots import make_subplots
from scipy import stats

logger = logging.getLogger(__name__)


def plot_timeseries(
    ds: xr.Dataset,
    variable: str,
    lat: float,
    lon: float,
    save_html: bool = False,
    filename: Optional[str] = None,
) -> go.Figure:
    """
    Create a beautiful, scientific time series plot of at a specific location.

    Parameters
    ----------
    ds : xarray.Dataset
        Dataset containing variable
    lat : float
        Latitude to extract
    lon : float
        Longitude to extract
    save_html : bool
        Whether to save the plot as HTML file
    filename : str | None
        Name of the HTML file to save

    Returns
    -------
    fig : plotly.graph_objects.Figure
        The plotly figure object
    """
    # Select nearest point to requested coordinates
    data_point = ds.sel(lat=lat, lon=lon, method="nearest")
    actual_lat = float(data_point.lat.values)
    actual_lon = float(data_point.lon.values)

    # Extract time series and compute
    timeseries = data_point[variable].compute()
    time = pd.to_datetime(timeseries.time.values)
    values = timeseries.values

    # Calculate statistics
    valid_mask = ~np.isnan(values)
    valid_values = values[valid_mask]
    valid_time = time[valid_mask]

    n_total = len(values)
    n_valid = len(valid_values)
    n_missing = n_total - n_valid

    # Statistical metrics
    if n_valid > 0:
        mean_val = np.mean(valid_values)
        std_val = np.std(valid_values)
        min_val = np.min(valid_values)
        max_val = np.max(valid_values)
        median_val = np.median(valid_values)

        # Linear trend
        if n_valid > 2:
            time_numeric = (valid_time - valid_time[0]).total_seconds() / (
                365.25 * 24 * 3600
            )  # years
            slope, intercept, r_value, p_value, std_err = stats.linregress(
                time_numeric, valid_values
            )
            trend_line = slope * time_numeric + intercept
        else:
            slope = np.nan
            trend_line = None
    else:
        mean_val = std_val = min_val = max_val = median_val = np.nan
        slope = np.nan
        trend_line = None

    # Calculate rolling statistics (annual moving average)
    df = pd.DataFrame({"value": values}, index=time)
    rolling_mean = df["value"].rolling(window=12, center=True, min_periods=6).mean()

    # Create subplots
    fig = make_subplots(
        rows=2,
        cols=1,
        row_heights=[0.75, 0.25],
        subplot_titles=(
            f"{variable.upper()} Time Series at ({actual_lat:.2f}°N, {actual_lon:.2f}°E)",
            "Annual Distribution (Box Plot by Decade)",
        ),
        vertical_spacing=0.12,
    )

    # Main time series with scatter points
    fig.add_trace(
        go.Scatter(
            x=time,
            y=values,
            mode="markers",
            name="Monthly values",
            marker=dict(
                size=4,
                color=values,
                colorscale="RdYlBu_r",
                showscale=True,
                colorbar=dict(title=f"{variable.upper()}<br>", x=1.15, len=0.7, y=0.85),
                line=dict(width=0.5, color="rgba(255,255,255,0.3)"),
            ),
            hovertemplate="<b>Date:</b> %{x|%Y-%m}<br><b>INDEX:</b> %{y:.2f} mm<extra></extra>",
        ),
        row=1,
        col=1,
    )

    # Rolling mean
    fig.add_trace(
        go.Scatter(
            x=rolling_mean.index,
            y=rolling_mean.values,
            mode="lines",
            name="12-month moving avg",
            line=dict(color="rgba(255, 100, 0, 0.8)", width=3),
            hovertemplate="<b>Date:</b> %{x|%Y-%m}<br><b>Moving Avg:</b> %{y:.2f} mm<extra></extra>",
        ),
        row=1,
        col=1,
    )

    # Mean line
    if not np.isnan(mean_val):
        fig.add_hline(
            y=mean_val,
            line_dash="dash",
            line_color="green",
            annotation_text=f"Mean: {mean_val:.2f} mm",
            annotation_position="right",
            row=1,
            col=1,
        )

        # Standard deviation bands
        fig.add_hrect(
            y0=mean_val - std_val,
            y1=mean_val + std_val,
            fillcolor="green",
            opacity=0.1,
            line_width=0,
            row=1,
            col=1,
        )

    # Trend line
    if trend_line is not None and not np.isnan(slope):
        fig.add_trace(
            go.Scatter(
                x=valid_time,
                y=trend_line,
                mode="lines",
                name=f"Linear trend ({slope:.3f} mm/year)",
                line=dict(color="red", width=2, dash="dot"),
                hovertemplate="<b>Trend:</b> %{y:.2f} mm<extra></extra>",
            ),
            row=1,
            col=1,
        )

    # Box plots by decade
    if n_valid > 0:
        df_valid = pd.DataFrame({"value": valid_values, "time": valid_time})
        df_valid["decade"] = (df_valid["time"].dt.year // 10) * 10

        decades = sorted(df_valid["decade"].unique())
        colors = [
            "#1f77b4",
            "#ff7f0e",
            "#2ca02c",
            "#d62728",
            "#9467bd",
            "#8c564b",
            "#e377c2",
            "#7f7f7f",
            "#bcbd22",
        ]

        for i, decade in enumerate(decades):
            decade_data = df_valid[df_valid["decade"] == decade]["value"]
            fig.add_trace(
                go.Box(
                    y=decade_data,
                    name=f"{decade}s",
                    marker_color=colors[i % len(colors)],
                    boxmean="sd",
                    hovertemplate="<b>%{x}</b><br>Value: %{y:.2f} mm<extra></extra>",
                ),
                row=2,
                col=1,
            )

    # Update layout
    fig.update_xaxes(
        title_text="Year",
        row=1,
        col=1,
        showgrid=True,
        gridcolor="rgba(128,128,128,0.2)",
    )
    fig.update_xaxes(title_text="Decade", row=2, col=1, showgrid=False)
    fig.update_yaxes(
        title_text=f"{variable}",
        row=1,
        col=1,
        showgrid=True,
        gridcolor="rgba(128,128,128,0.2)",
    )
    fig.update_yaxes(
        title_text=f"{variable}",
        row=2,
        col=1,
        showgrid=True,
        gridcolor="rgba(128,128,128,0.2)",
    )

    # Statistics text box
    stats_text = (
        f"<b>Statistical Summary</b><br>"
        f"Period: {time[0].strftime('%Y-%m')} to {time[-1].strftime('%Y-%m')}<br>"
        f"Valid points: {n_valid} / {n_total} ({100 * n_valid / n_total:.1f}%)<br>"
        f"Missing: {n_missing}<br>"
        f"<br>"
        f"<b>Values (mm)</b><br>"
        f"Mean: {mean_val:.2f} ± {std_val:.2f}<br>"
        f"Median: {median_val:.2f}<br>"
        f"Min: {min_val:.2f}<br>"
        f"Max: {max_val:.2f}<br>"
        f"<br>"
        f"<b>Trend</b><br>"
        f"Slope: {slope:.3f} mm/year"
        if not np.isnan(slope)
        else "N/A"
    )

    fig.add_annotation(
        xref="paper",
        yref="paper",
        x=0.02,
        y=0.98,
        xanchor="left",
        yanchor="top",
        text=stats_text,
        showarrow=False,
        bgcolor="rgba(255, 255, 255, 0.9)",
        bordercolor="rgba(0, 0, 0, 0.3)",
        borderwidth=1,
        borderpad=10,
        font=dict(size=10, family="monospace"),
        align="left",
    )

    # Overall layout
    fig.update_layout(
        height=900,
        showlegend=True,
        legend=dict(
            orientation="v",
            yanchor="top",
            y=0.65,
            xanchor="left",
            x=1.02,
            bgcolor="rgba(255, 255, 255, 0.8)",
            bordercolor="rgba(0, 0, 0, 0.3)",
            borderwidth=1,
        ),
        template="plotly_white",
        font=dict(family="Arial, sans-serif", size=12),
        title=dict(
            text=f"<b>{variable}</b><br><sub>Location: {actual_lat:.2f}°N, {actual_lon:.2f}°E</sub>",
            x=0.5,
            xanchor="center",
            font=dict(size=18),
        ),
        hovermode="x unified",
    )

    if save_html:
        if filename is None:
            filename = f"{variable}_timeseries.html"
        fig.write_html(filename)
        logger.info(f"Plot saved as {filename}")

    return fig


def analyze_dataset(
    ds: xr.Dataset, variable: Optional[str] = None, output_file: Optional[str] = None
) -> Dict[str, Any]:
    """
    Fast analysis of dataset variable to check for valid values.

    Parameters
    ----------
    ds : xarray.Dataset
        Dataset to analyze
    variable : str, optional
        Variable name to analyze. If None, uses the first data variable
    output_file : str, optional
        Path to save the analysis output. If None, prints to console
    """
    lines = []  # Collect all output lines here

    # Auto-detect variable if not provided
    if variable is None:
        data_vars = list(ds.data_vars)
        if len(data_vars) == 0:
            lines.append("Error: No data variables found in dataset")
            output_text = "\n".join(lines)
            if output_file:
                with open(output_file, "w") as f:
                    f.write(output_text)
            return {"error": "No data variables found in dataset"}
        variable = data_vars[0]
        lines.append(f"Auto-detected variable: {variable}")

    # Check if variable exists
    if variable not in ds.data_vars:
        lines.append(f"Error: Variable '{variable}' not found in dataset")
        lines.append(f"Available variables: {list(ds.data_vars)}")
        output_text = "\n".join(lines)
        if output_file:
            with open(output_file, "w") as f:
                f.write(output_text)
        return {"error": f"Variable '{variable}' not found in dataset"}

    lines.append("=" * 60)
    lines.append(f"{variable.upper()} DATASET ANALYSIS")
    lines.append("=" * 60)

    # Basic info
    lines.append(f"Dataset dimensions: {ds.dims}")

    # Check if 'time' dimension exists
    time_dim = "time" if "time" in ds.dims else None
    if time_dim:
        lines.append(
            f"Time range: {ds[time_dim].values[0]} to {ds[time_dim].values[-1]}"
        )
        lines.append(f"Total timesteps: {len(ds[time_dim])}")
    else:
        lines.append("Note: No 'time' dimension found in dataset")

    # Get the data variable
    data_var = ds[variable]

    # Initialize statistics dictionary
    stats: Dict[str, Any] = {
        "variable": variable,
        "dimensions": dict(ds.dims),
        "total_size": int(data_var.size),
        "has_time_dim": time_dim is not None,
    }

    # Sample a few time slices without loading all data
    if time_dim:
        sample_times = [0, len(ds[time_dim]) // 2, -1]  # First, middle, last

        lines.append("\n" + "=" * 60)
        lines.append("SAMPLING ANALYSIS (First, Middle, Last timesteps)")
        lines.append("=" * 60)

        stats["time_samples"] = []

        for idx in sample_times:
            time_val = ds[time_dim].values[idx]
            data_slice = data_var.isel({time_dim: idx}).compute()

            valid_count = np.sum(~np.isnan(data_slice.values))
            nan_count = np.sum(np.isnan(data_slice.values))
            total = data_slice.size

            sample_stats = {
                "index": int(idx),
                "time": str(time_val),
                "shape": data_slice.shape,
                "total": int(total),
                "valid": int(valid_count),
                "nan": int(nan_count),
                "pct_valid": float(100 * valid_count / total),
            }

            lines.append(f"\n--- Time: {time_val} (index {idx}) ---")
            lines.append(f"  Shape: {data_slice.shape}")
            lines.append(f"  Total values: {total}")
            lines.append(f"  Valid (non-NaN) values: {valid_count}")
            lines.append(f"  NaN values: {nan_count}")
            lines.append(f"  Percentage valid: {sample_stats['pct_valid']:.2f}%")

            if np.any(~np.isnan(data_slice.values)):
                valid_data = data_slice.values[~np.isnan(data_slice.values)]
                sample_stats.update(
                    {
                        "min": float(np.min(valid_data)),
                        "max": float(np.max(valid_data)),
                        "mean": float(np.mean(valid_data)),
                        "std": float(np.std(valid_data)),
                    }
                )
                lines.append(f"  Min value: {sample_stats['min']:.2f}")
                lines.append(f"  Max value: {sample_stats['max']:.2f}")
                lines.append(f"  Mean value: {sample_stats['mean']:.2f}")
                lines.append(f"  Std value: {sample_stats['std']:.2f}")
            else:
                logger.warning("  ⚠️  ALL VALUES ARE NaN!")

            stats["time_samples"].append(sample_stats)

        # Check a few random timesteps
        lines.append("\n" + "=" * 60)
        lines.append("RANDOM SAMPLE CHECK (5 random timesteps)")
        lines.append("=" * 60)

        random_indices = np.random.choice(
            len(ds[time_dim]), size=min(5, len(ds[time_dim])), replace=False
        )
        stats["random_samples"] = []

        for idx in random_indices:
            time_val = ds[time_dim].values[idx]
            data_slice = data_var.isel({time_dim: idx}).compute()
            valid_count = np.sum(~np.isnan(data_slice.values))
            pct_valid = 100 * valid_count / data_slice.size

            stats["random_samples"].append(
                {
                    "index": int(idx),
                    "time": str(time_val),
                    "valid": int(valid_count),
                    "total": int(data_slice.size),
                    "pct_valid": float(pct_valid),
                }
            )

            lines.append(
                f"  {time_val}: {valid_count}/{data_slice.size} valid ({pct_valid:.1f}%)"
            )
    else:
        # If no time dimension, just analyze the whole array
        lines.append("\n" + "=" * 60)
        lines.append("DATA ANALYSIS (No time dimension)")
        lines.append("=" * 60)
        lines.append(f"Shape: {data_var.shape}")
        lines.append("Computing statistics (this may take a moment)...")
        data_computed = data_var.compute()

        valid_count = np.sum(~np.isnan(data_computed.values))
        nan_count = np.sum(np.isnan(data_computed.values))
        total = data_computed.size

        lines.append(f"Total values: {total}")
        lines.append(f"Valid (non-NaN) values: {valid_count}")
        lines.append(f"NaN values: {nan_count}")
        lines.append(f"Percentage valid: {100 * valid_count / total:.2f}%")

        stats["no_time_analysis"] = {
            "shape": data_var.shape,
            "total": int(total),
            "valid": int(valid_count),
            "nan": int(nan_count),
            "pct_valid": float(100 * valid_count / total),
        }

    # Overall statistics (computed efficiently)
    lines.append("\n" + "=" * 60)
    lines.append("OVERALL STATISTICS")
    lines.append("=" * 60)

    # Check if entire dataset is NaN (without loading everything)
    lines.append("\nComputing overall statistics (this may take a moment)...")

    total_valid = (~np.isnan(data_var)).sum().compute().values
    total_size = data_var.size

    lines.append(f"Total grid points across all times: {total_size:,}")
    lines.append(f"Valid (non-NaN) values: {total_valid:,}")
    lines.append(f"NaN values: {total_size - total_valid:,}")
    lines.append(f"Percentage valid: {100 * total_valid / total_size:.2f}%")

    stats["overall"] = {
        "total_size": int(total_size),
        "valid": int(total_valid),
        "nan": int(total_size - total_valid),
        "pct_valid": float(100 * total_valid / total_size),
    }

    if total_valid > 0:
        lines.append("\n✓ Dataset contains valid values")
        # Get some overall stats
        min_val = data_var.min().compute().values
        max_val = data_var.max().compute().values
        lines.append(f"Overall min: {min_val:.2f}")
        lines.append(f"Overall max: {max_val:.2f}")

        stats["overall"].update({"min": float(min_val), "max": float(max_val)})
    else:
        logger.warning("\n✗ WARNING: ENTIRE DATASET IS NaN!")

    # Check for division warnings - look for suspicious values
    lines.append("\n" + "=" * 60)
    lines.append("CHECKING FOR SUSPICIOUS VALUES")
    lines.append("=" * 60)

    # Sample first 10 timesteps or entire array if no time dimension
    if time_dim:
        sample = data_var.isel(
            {time_dim: slice(0, min(10, len(ds[time_dim])))}
        ).compute()
    else:
        sample = data_var.compute()

    has_inf = np.any(np.isinf(sample.values))
    has_zero = np.any(sample.values == 0)
    has_negative = np.any(sample.values < 0)

    stats["suspicious_values"] = {
        "has_inf": bool(has_inf),
        "has_zero": bool(has_zero),
        "has_negative": bool(has_negative),
    }

    lines.append(f"Contains Inf values: {has_inf}")
    lines.append(f"Contains zero values: {has_zero}")
    lines.append(f"Contains negative values: {has_negative}")

    if has_inf:
        logger.warning("⚠️  Found Inf values - likely division by zero!")
    if has_negative:
        logger.warning(
            f"⚠️  Found negative values - check if this is expected for {variable}"
        )

    lines.append("\n" + "=" * 60)
    lines.append("ANALYSIS COMPLETE")
    lines.append("=" * 60)

    if output_file:
        lines.append(f"\nLog saved to: {output_file}")

    # Build final text
    output_text = "\n".join(lines)

    # Save to file if requested
    if output_file:
        with open(output_file, "w") as f:
            f.write(output_text)
    return stats


def inspect_dataset(
    ds: xr.Dataset,
    name: str,
    var: str = "r95ptot",
) -> None:
    _ = analyze_dataset(
        ds=ds, variable=var, output_file=f"{var}_analysis_{name.lower()}.txt"
    )

    plot_timeseries(
        ds,
        var,
        lat=38.948,
        lon=-3.236,
        save_html=True,
        filename=f"{var}_timeseries_{name.lower()}.html",
    )
