import os
import pathlib
import pytest
from unittest.mock import MagicMock, patch
import xarray as xr
import numpy as np
import pandas as pd
from ingestion_pipeline.ingestion import IngestionPipeline

@pytest.fixture
def mock_pipeline(tmp_path):
    """Create a mock pipeline with a temporary directory."""
    with patch("ingestion_pipeline.ingestion.cdsapi.Client"), \
         patch("ingestion_pipeline.ingestion.S3Handler"), \
         patch("ingestion_pipeline.ingestion.S3Config.from_env"):
        pipeline = IngestionPipeline(
            dataset="test-dataset",
            variable="tas_2m",
            saving_main_directory=str(tmp_path)
        )
        return pipeline

def test_cleanup_directories(mock_pipeline):
    """Test that cleanup_directories removes files and directories."""
    base_dir = pathlib.Path(mock_pipeline.saving_main_directory)
    
    # Create some dummy files and directories
    subdir = base_dir / "test_subdir"
    subdir.mkdir(parents=True)
    file1 = subdir / "file1.txt"
    file1.write_text("hello")
    file2 = base_dir / "file2.txt"
    file2.write_text("world")
    
    assert file1.exists()
    assert file2.exists()
    assert subdir.exists()
    
    mock_pipeline.cleanup_directories()
    
    assert not file1.exists()
    assert not file2.exists()
    assert not subdir.exists()
    # The base_dir itself might still exist depending on implementation (os.walk topdown=False)
    # Our implementation removes directories if they are empty.
    # Since base_dir was empty after deleting everything, it should be removed too.
    assert not base_dir.exists()

def test_run_pipeline_calls_cleanup(mock_pipeline):
    """Test that run_pipeline calls cleanup at the end."""
    # Mock all heavy operations
    mock_pipeline.download = MagicMock(return_value=["file1.nc"])
    mock_pipeline.homogenize = MagicMock(return_value="file1_hom.nc")
    
    # Create a dummy dataset
    ds = xr.Dataset(
        {"tas": (("time", "lat", "lon"), np.random.rand(1, 10, 10))},
        coords={
            "time": [pd.to_datetime("2020-01-01")],
            "lat": np.linspace(-90, 90, 10),
            "lon": np.linspace(-180, 180, 10),
        }
    )
    ds.attrs["dcterms:valid"] = "2020-01-01/2020-01-01"
    
    with patch("xarray.open_mfdataset", return_value=ds), \
         patch("ingestion_pipeline.ingestion.apply_dublin_core_metadata", return_value=ds), \
         patch("ingestion_pipeline.ingestion.chunk_dataset", return_value=ds), \
         patch("ingestion_pipeline.ingestion.aggregate_regions", return_value=ds):
        
        # We need to mock s3_handler.check_zarr_exists to False to trigger download path
        mock_pipeline.s3_handler.check_zarr_exists.return_value = False
        mock_pipeline.s3_handler.write_ds = MagicMock()
        
        # Mock cleanup_directories to verify it's called
        mock_pipeline.cleanup_directories = MagicMock()
        
        mock_pipeline.run_pipeline()
        
        # Verify cleanup was called
        mock_pipeline.cleanup_directories.assert_called_once()
        
        # Verify dataset was closed (actually we called ds.close())
        # We can't easily check if the returned dataset was closed because we mocked it,
        # but we can verify the close call if we mock the dataset object itself.
