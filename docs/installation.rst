Installation
============

The easiest way to install and run the ``ingestion_pipeline`` is using a conda/mamba environment.

1. **Create the environment:**

   .. code-block:: bash

      make mamba-create-env

2. **Activate the environment:**

   .. code-block:: bash

      mamba activate ingestion_pipeline

3. **Install the package in editable mode:**

   .. code-block:: bash

      pip install -e .

CDS API Configuration
--------------------

The pipeline requires access to the Copernicus Climate Data Store (CDS). You must have a ``.cdsapirc`` file in your home directory with your API key and URL.

See the `CDS Help Page <https://cds.climate.copernicus.eu/api-how-to>`_ for more information.
