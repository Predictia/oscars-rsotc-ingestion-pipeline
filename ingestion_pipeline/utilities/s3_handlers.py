import logging
import os
from fnmatch import fnmatch
from pathlib import Path
from typing import Any

import boto3
import dask
import fsspec
import s3fs
import xarray as xr
from dotenv import load_dotenv
from pydantic_settings import BaseSettings
from s3fs.core import S3FileSystem
from typing_extensions import Self

dask.config.set(scheduler="threads")

logger = logging.getLogger(__name__)


class S3Config(BaseSettings):
    """
    Configuration for S3 and Zarr I/O operations.

    Handles connection details and credentials for S3-compatible object storage
    (e.g., Contabo, AWS, MinIO).

    Attributes
    ----------
    bucket_name : str
        S3 bucket name.
    endpoint_url : str
        S3 endpoint URL (e.g., "https://s3.contabo.com").
    access_key : str
        S3 access key for authentication.
    secret_key : str
        S3 secret key for authentication.
    region : str
        S3 region.

    Notes
    -----
    Configuration can be loaded from environment variables with "S3_" prefix:
    - S3_BUCKET_NAME
    - S3_ENDPOINT_URL
    - S3_ACCESS_KEY
    - S3_SECRET_KEY
    - S3_REGION
    """

    bucket_name: str
    endpoint_url: str
    access_key: str
    secret_key: str
    region: str

    class Config:
        env_prefix = "S3_"  # e.g. S3_BUCKET_NAME, S3_ENDPOINT_URL
        case_sensitive = False

    @classmethod
    def from_env(cls) -> Self:
        """
        Load configuration from environment variables.

        Handles retrieving S3 configuration from environment variables
        with the "S3_" prefix.

        Returns
        -------
        S3Config
            Initialized configuration object.

        Raises
        ------
        ValueError
            If required S3 environment variables are missing.
        """
        load_dotenv()
        env_values = {
            "bucket_name": os.getenv("S3_BUCKET_NAME"),
            "endpoint_url": os.getenv("S3_ENDPOINT_URL"),
            "access_key": os.getenv("S3_ACCESS_KEY"),
            "secret_key": os.getenv("S3_SECRET_KEY"),
            "region": os.getenv("S3_REGION"),
        }

        if not all(env_values.values()):
            missing_keys = [key for key, value in env_values.items() if not value]
            raise ValueError(
                f"Missing required environment variables: {', '.join(missing_keys)}"
            )

        return cls(**env_values)


class S3Handler:
    """
    Unified handler for I/O operations with xindices results.

    Manages reading input climate data from S3 Zarr stores and writing
    computed indices to S3 or local storage.

    Attributes
    ----------
    s3_config : S3Config
        S3 connection configuration.
    fs : fsspec.AbstractFileSystem
        Filesystem instance for S3 operations.

    Examples
    --------
    >>> config = S3Config.from_env("s3.env")
    >>> handler = S3Handler(config)
    >>> dataset = handler.read_variable("tas", "s3://bucket/climate.zarr")
    >>> handler.write_ds(result, "tx35_monthly.zarr")
    """

    def __init__(
        self,
        s3_config: S3Config,
        **s3_kwargs: Any,
    ) -> None:
        self.s3_config = s3_config
        self.s3_kwargs = s3_kwargs

        # Initialize S3 filesystem
        self.fs: S3FileSystem = fsspec.filesystem(
            "s3",
            key=s3_config.access_key,
            secret=s3_config.secret_key,
            client_kwargs={
                "endpoint_url": s3_config.endpoint_url,
                "region_name": s3_config.region,
            },
            config_kwargs={
                "max_pool_connections": 50,
            },
        )
        self.s3 = s3fs.S3FileSystem(
            key=s3_config.access_key,
            secret=s3_config.secret_key,
            client_kwargs=dict(
                endpoint_url=s3_config.endpoint_url,
                region_name=s3_config.region,
            ),
            config_kwargs={
                "max_pool_connections": 50,
            },
        )

        logger.info(f"Initialized S3Handler for bucket: {s3_config.bucket_name}")

    @property
    def base_path(self) -> str:
        """Return the base S3 path of the bucket."""
        return f"s3://{self.s3_config.bucket_name}/"

    def get_s3_path(self, filename: str) -> str:
        """Construct an S3 path from a filename."""
        return f"{self.base_path.rstrip('/')}/{filename}"

    def list_files(
        self,
        bucket: str | None = None,
        suffix: str | None = None,
        pattern: str | None = None,
    ) -> list[str]:
        """List files in the S3 bucket, optionally filtering by suffix and/or pattern."""
        if bucket is None:
            bucket = self.s3_config.bucket_name
        all_files = self.fs.ls(bucket, detail=False)
        files = [f"s3://{f}" for f in all_files]

        # Filter by suffix
        if suffix:
            files = [f for f in files if f.endswith(suffix)]

        # Filter by glob-like pattern (e.g., *ERA5_gridded.zarr)
        if pattern:
            files = [f for f in files if fnmatch(Path(f).name, pattern)]

        return files

    def read_file(
        self,
        zarr_path: str,
        variable: str | None = None,
        time_slice: slice | None = None,
        spatial_slice: dict[str, slice] | None = None,
        chunks: dict[str, Any] | None = None,
    ) -> xr.Dataset:
        """
        Read a specific variable from a Zarr dataset on S3.

        Parameters
        ----------
        variable : str
            Name of the variable to read.
        zarr_path : str
            S3 path to the Zarr store (e.g., "s3://bucket/climate.zarr").
        time_slice : slice, optional
            Time index slice for temporal subsetting.
        spatial_slice : dict[str, slice], optional
            Spatial slices by dimension (e.g., {"lat": slice(0, 100)}).
        chunks : dict[str, Any], optional
            Dask chunking scheme. If None, opens without chunking.

        Returns
        -------
        xr.Dataset
            Dataset containing the requested variable.
        """
        try:
            # Open the zarr store
            ds = xr.open_zarr(
                self.fs.get_mapper(str(zarr_path)),
                consolidated=True,
                chunks=chunks or {},
            )

            # Select variable
            if variable:
                if variable not in ds:
                    raise ValueError(
                        f"Variable '{variable}' not found in dataset. "
                        f"Available variables: {list(ds.data_vars)}"
                    )

                ds = ds[[variable]]

            # Apply time slice
            if time_slice is not None:
                ds = ds.isel(time=time_slice)

            # Apply spatial slices
            if spatial_slice is not None:
                ds = ds.sel(**spatial_slice)

            logger.info(f"Successfully loaded {zarr_path} with shape: {dict(ds.sizes)}")
            return ds

        except Exception as e:
            logger.error(f"Error reading {zarr_path} from S3: {e}")
            raise

    def write_ds(
        self,
        dataset: xr.Dataset,
        output_path: str,
        overwrite: bool = False,
        num_workers: int = 4,
        append_dim: str | None = None,
        encoding: dict[str, Any] | None = None,
    ) -> bool:
        """
        Write or append a dataset to S3 as a Zarr dataset.

        Parameters
        ----------
        dataset : xr.Dataset
            Dataset to write.
        output_path : str
            S3 path for the output.
        overwrite : bool, optional
            Whether to overwrite existing data. Default is False.
        num_workers : int, optional
            Number of concurrent upload workers. Default is 4.
        append_dim : str, optional
            Dimension along which to append data. If provided, mode='a' is used.
        encoding : dict[str, Any], optional
            Zarr encoding for each variable.

        Returns
        -------
        bool
            True if the upload was successful, False otherwise.
        """
        if not self.is_s3_path(output_path):
            output_path = self.get_s3_path(Path(output_path).name)
        bucket_name, s3_key = self.split_s3_path(output_path)

        prefix = f"{bucket_name}/{s3_key}".rstrip("/")
        if not overwrite and not append_dim and self.check_zarr_exists(s3_key):
            logger.info(f"Upload skipped (exists): s3://{prefix}")
            return True

        try:
            mapper = fsspec.get_mapper(
                output_path,
                key=self.s3_config.access_key,
                secret=self.s3_config.secret_key,
                client_kwargs=dict(
                    endpoint_url=self.s3_config.endpoint_url,
                    region_name=self.s3_config.region,
                ),
                config_kwargs={
                    "retries": {"max_attempts": 15, "mode": "adaptive"},
                    "max_pool_connections": max(50, 4 * num_workers),
                },
            )
            if append_dim:
                logger.info(f"Appending to s3://{prefix} along dim {append_dim}")
                dataset.to_zarr(
                    mapper,
                    mode="a",
                    append_dim=append_dim,
                    consolidated=True,
                    encoding=encoding,
                )
            else:
                logger.info(f"Writing to s3://{prefix}")
                dataset.to_zarr(mapper, mode="w", consolidated=True, encoding=encoding)
            return True
        except Exception as e:
            logger.error(f"Failed to upload to s3://{prefix}: {e}")
            return False

    def update_zarr_ds(
        self,
        dataset: xr.Dataset,
        output_path: str,
        append_dim: str,
        reindex_dim: str | None = None,
        attrs_to_update: list[str] = [],
        num_workers: int = 4,
    ) -> bool:
        """
        Append or update a Zarr dataset at the given path.

        This method opens an existing Zarr dataset and appends the provided
        `dataset` along the given `append_dim`. Optionally it will reindex the
        new dataset to match an existing dimension in the destination dataset
        (useful when ensuring coordinate alignment), and it updates selected
        attributes before writing. The destination must already exist.

        Parameters
        ----------
        dataset : xr.Dataset
            The dataset to append to the existing Zarr store.
        output_path : str
            The target Zarr store path. Can be a full S3 path (``s3://bucket/key``)
            or a local filename; non-S3 paths will be translated to the
            configured bucket root.
        append_dim : str
            Name of the dimension along which to append (passed to
            ``xr.Dataset.to_zarr(..., append_dim=...)``).
        reindex_dim : str or None, optional
            If provided, reindex the incoming `dataset` to match the values of
            this dimension from the existing dataset in the store. Default is
            ``None`` (no reindexing).
        attrs_to_update : list[str], optional
            List of attribute keys to copy from the incoming `dataset` into
            the stored dataset's attributes. Only keys present in this list
            will be taken from `dataset.attrs` and merged into the stored
            attributes. Default is an empty list.
        num_workers : int, optional
            Number of workers used to configure the fsspec mapper's connection
            pool size (used to tune parallel uploads). Default is 4.

        Returns
        -------
        bool
            ``True`` when the append/update operation succeeded, ``False`` on
            failure.

        Raises
        ------
        FileNotFoundError
            If the destination Zarr path does not exist.
        """
        if not self.path_exists(output_path):
            logger.info(f"Zarr dataset {output_path} does not exist. Creat it first.")
            raise FileNotFoundError(f"Zarr dataset {output_path} does not exist.")

        if not self.is_s3_path(output_path):
            output_path = self.get_s3_path(Path(output_path).name)

        mapper = fsspec.get_mapper(
            output_path,
            key=self.s3_config.access_key,
            secret=self.s3_config.secret_key,
            client_kwargs=dict(
                endpoint_url=self.s3_config.endpoint_url,
                region_name=self.s3_config.region,
            ),
            config_kwargs={
                "retries": {"max_attempts": 15, "mode": "adaptive"},
                "max_pool_connections": max(50, 4 * num_workers),
            },
        )
        logger.info(f"Appending to Zarr at {output_path} along dimension {append_dim}")

        # Reindex new dataset
        ds_old = xr.open_zarr(mapper, consolidated=False)
        if reindex_dim is not None and reindex_dim in ds_old:
            dataset = dataset.reindex({reindex_dim: ds_old[reindex_dim].values})

        # Special case for time_filter if it exists (legacy alignment)
        if "time_filter" in ds_old:
            filters_full = ds_old.time_filter.values
            dataset = dataset.reindex(time_filter=filters_full)

        # Update attributes
        merged_attrs = ds_old.attrs.copy()
        # Update with attributes from the incoming dataset if specified
        merged_attrs.update(
            {k: dataset.attrs[k] for k in attrs_to_update if k in dataset.attrs}
        )

        # Always update Dublin Core dynamic attributes if they exist in incoming dataset
        dc_attributes = [
            "dcterms:temporal",
            "dcterms:valid",
            "dc:coverage",
            "dcterms:modified",
        ]
        for k in dc_attributes:
            if k in dataset.attrs:
                merged_attrs[k] = dataset.attrs[k]

        # Explicitly remove legacy attributes if they are still present
        for k in ["start_date", "end_date", "original_frequency", "last_updated"]:
            merged_attrs.pop(k, None)

        dataset.attrs = merged_attrs

        try:
            dataset.to_zarr(mapper, mode="a", append_dim=append_dim, consolidated=True)
        except Exception as e:
            logger.error(f"Error appending to Zarr at {output_path}: {e}")
            return False
        return True

    def file_exists(self, filename: str) -> bool:
        """Check if a file already exists in S3 (relative to bucket root)."""
        file_path = f"{self.s3_config.bucket_name}/{filename}"
        return self.fs.exists(file_path)

    def path_exists(self, path: str) -> bool:
        """Check if a path exists in S3."""
        if path.startswith("s3://"):
            path = path.replace("s3://", "")
        return self.fs.exists(path)

    def remove_path(self, path: str) -> None:
        """
        Remove a path from S3 (recursive).

        Parameters
        ----------
        path : str
            The S3 path or key to remove.
        """
        if path.startswith("s3://"):
            path = path.replace("s3://", "")

        if self.s3.exists(path):
            logger.info(f"Removing {path} from S3")
            try:
                self.s3.rm(path, recursive=True)
            except Exception as e:
                logger.error(f"Error removing {path} from S3: {e}")
        else:
            logger.info(f"{path} does not exist in S3")

    def upload_file(self, local_path: Path | str, s3_key: str) -> None:
        """
        Upload a local file to S3.

        Parameters
        ----------
        local_path : Path or str
            Path to the local file.
        s3_key : str
            The target S3 key.
        """
        s3_client = boto3.client(
            "s3",
            endpoint_url=self.s3_config.endpoint_url,
            aws_access_key_id=self.s3_config.access_key,
            aws_secret_access_key=self.s3_config.secret_key,
            region_name=self.s3_config.region,
        )
        s3_client.upload_file(str(local_path), self.s3_config.bucket_name, s3_key)

    def check_zarr_exists(self, s3_key: str) -> bool:
        """
        Check if a Zarr dataset already exists in S3.

        Checks for either consolidated metadata (.zmetadata) or
        at least one .zarray within the prefix.

        Parameters
        ----------
        s3_key : str
            The S3 key to check.

        Returns
        -------
        bool
            True if effectively a Zarr store exists at that prefix.
        """
        prefix = f"{self.s3_config.bucket_name}/{s3_key}".rstrip("/")
        if self.fs.exists(f"{prefix}/.zmetadata"):
            return True
        if self.fs.exists(f"{prefix}/.zgroup"):
            arrays = self.fs.find(prefix)
            return any(f.endswith(".zarray") for f in arrays)
        return False

    def inspect_zarr_metadata_in_s3(self, zarr_path: str) -> dict[str, Any]:
        """
        Inspect the metadata of a Zarr dataset in S3.

        Parameters
        ----------
        zarr_path : str
            Full S3 path to the Zarr store.

        Returns
        -------
        dict
            Dictionary containing dataset attributes and variables.
        """
        try:
            ds = xr.open_zarr(self.fs.get_mapper(zarr_path), consolidated=True)
            return {
                "attrs": ds.attrs,
                "variables": list(ds.data_vars),
                "coords": list(ds.coords),
                "dims": dict(ds.sizes),
            }
        except Exception as e:
            logger.error(f"Failed to inspect Zarr metadata at {zarr_path}: {e}")
            return {}

    @staticmethod
    def split_s3_path(s3_path: str) -> tuple[str, str]:
        # Remove the prefix if it exists
        if s3_path.startswith("s3://"):
            s3_path = s3_path[5:]

        # Split into bucket and key
        parts = s3_path.split("/", 1)  # only split on the first slash
        bucket = parts[0]
        key = parts[1] if len(parts) > 1 else ""

        return bucket, key

    @staticmethod
    def is_s3_path(path: str) -> bool:
        if not path.startswith("s3://"):
            return False
        # Check that there is a bucket name after "s3://"
        remaining = path[5:]
        return bool(remaining and not remaining.startswith("/"))
