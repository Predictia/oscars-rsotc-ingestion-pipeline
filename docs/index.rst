Welcome to ingestion_pipeline's documentation!
==============================================

The ingestion_pipeline is a robust tool designed for downloading and homogenizing OSCARS-RSOTC climate data.
It simplifies the process of retrieving data from the Copernicus Climate Data Store (CDS) and preparing it for analysis and visualization.

Project Overview
----------------

This project provides an automated pipeline to:
* Download ERA5 and other climate datasets.
* Homogenize units, coordinates, and metadata.
* Aggregate data spatially for various region sets (e.g., NUTS).
* Store processed data in S3-compatible storage using Zarr format.

.. toctree::
   :maxdepth: 2
   :caption: Getting Started:

   installation
   usage

.. toctree::
   :maxdepth: 2
   :caption: API Reference:

   _api/index

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
