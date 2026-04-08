Usage
=====

The ``ingestion_pipeline`` provides a Command Line Interface (CLI) to manage data downloads and processing.

Download Command
---------------

To download historical data for a specific variable and area:

.. code-block:: bash

   python ingestion_pipeline/cli.py download \
       --dataset "derived-era5-single-levels-daily-statistics" \
       --variable "2m_temperature" \
       --area "75;-30;30;50" \
       --start-date "2020-01-01" \
       --end-date "2022-12-31" \
       --max-workers 4 \
       --saving-temporal-aggregation "daily" \
       --saving-main-dir "/path/to/data" \
       --saving-chunks-size "500;50;50" \
       --s3-information-path ".env" \
       --overwrite

Compute Derived Indices
----------------------

To compute specialized indices from pre-downloaded ERA5 data:

.. code-block:: bash

   python ingestion_pipeline/cli.py compute_derived_indices \
       --indice "tx35" \
       --temporal-aggregation "monthly" \
       --s3-information-path ".env" \
       --overwrite

Workflow run provenance with RO-Crate (Research Object Crate)
-------------------------------------------

Record (prospective & retrospective) provenance of a `ingestion-pipeline`
workflow run as a RO-Crate package. Every workflow run encompases the execution
of the two above commands (download and compute derived indices). The final crate
can be returned as a folder and/or as a zip file.

.. code-block:: bash

    pixi run ingestion-pipeline generate-crate \
        --workflow-spec <path-to-the-argo-specification>.yaml \
        --static-metadata-file <path-to-the-static-metadata-info>.yaml \
        --output-crate-zip <path-to-store-the-crate>.zip \
        --rocrate-profile <rocrate-profile> \
        --rocrate-gen-preview  <True|False> \ # Generate a HTML preview file for the crate
        --output-crate-path <path-for-rocrate-folder> \
        --output-crate-zip <path-for-rocrate-zip> \
        --pattern <pattern>

where:

- ``<path-to-the-argo-specification.yaml>`` must adhere to `Argo Workflow templates`_.
- ``<path-to-the-yaml-static-metadata-info>`` adheres to the format shown in the `sample configuration`_.
- ``<rocrate-profile>`` allows to select among the two RO-Crate profiles to generate the provenance of the workflow run, either ``workflow-run-crate-0.5`` or ``provenance-run-crate-0.5``. The identifier complies with the `rocrate-validate profiles command`_.
- ``<pattern>`` is the pattern to match S3 objects for publishing (default: ``"*_NUTS*.zarr"``).

.. _Argo Workflow templates: https://argo-workflows.readthedocs.io/en/latest/workflow-templates/
.. _sample configuration: ./ingestion_pipeline/provenance/config/provenance_metadata_static_info.yaml.sample
.. _rocrate-validate profiles command: https://github.com/crs4/rocrate-validator

Argo server credentials
~~~~~~~~~~~~~~~~~~~~~~~

In order to generate the retrospective provenance, this module gets workflow execution data from the Argo server API. For this to work, several pieces of information are required in order to access to the Argo API (which usually requires authentication), and the specific details of the workflow run, in particular the identifier and namespace where it has been executed. All the Argo-related information is currently is provided through a dotenv approach (``.env`` file by default):

.. code-block:: bash

    ARGO_SERVER="<argo-server-url>"
    ARGO_NAMESPACE="<workflow-namespace>"
    ARGO_TOKEN="<token>"
    ARGO_SECURE=true
    ARGO_INSECURE_SKIP_VERIFY=false
    ARGO_WORKFLOW="<workflow-name>"
