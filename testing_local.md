# Local Testing with Docker

This guide explains how to build the Docker image and run the ingestion pipeline locally to simulate the execution environment used in the Argo workflows (`argo/workflow-template.yaml`).

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/) installed and running.
- (Optional) `.env` file with S3 credentials (see `.env-sample`).

## 1. Build the Docker Image

Build the container image using the `Dockerfile` in the root of the repository:

```bash
docker build -t ingestion-pipeline:latest .
```

Alternatively, if you have `pixi` installed, you can use the task defined in `pixi.toml`:

```bash
pixi run docker-build
```

## 2. Run the Container Locally

To run the container, you need to provide the necessary environment variables and mount a local directory for data storage, mirroring the `/data` volume used in Argo.

### Environment Variables

The pipeline requires S3 credentials to work. You can pass them as environment variables using the `-e` flag:

- `S3_ENDPOINT_URL`: The URL of the S3 service.
- `S3_REGION`: The region (e.g., `garage`).
- `S3_BUCKET_NAME`: The target bucket.
- `S3_ACCESS_KEY`: Your access key.
- `S3_SECRET_KEY`: Your secret key.

### Volume Mounting

Mount a local folder to `/data` inside the container using the `-v` flag to persist the downloaded and computed data.

### Example: Run the Download Task

The following command simulates the `download-task` from the Argo template:

```bash
docker run --rm \
  -e S3_ENDPOINT_URL="https://s3.eu-north-1.predictia.es" \
  -e S3_REGION="garage" \
  -e S3_BUCKET_NAME="oscars-test" \
  -e S3_ACCESS_KEY="YOUR_ACCESS_KEY" \
  -e S3_SECRET_KEY="YOUR_SECRET_KEY" \
  -e CDS_API_KEY="YOUR_CDS_API_KEY" \
  -v $(pwd)/data:/data \
  ingestion-pipeline:latest \
  download \
  --dataset "derived-era5-single-levels-daily-statistics" \
  --variable "tas_None" \
  --area "75;-30;30;50" \
  --saving-temporal-aggregation "monthly" \
  --saving-main-dir "/data/oscars-rsotc"
```

### Example: Run the Compute Task

The following command simulates the `compute-task` from the Argo template:

```bash
docker run --rm \
  -e S3_ENDPOINT_URL="https://s3.eu-north-1.predictia.es" \
  -e S3_REGION="garage" \
  -e S3_BUCKET_NAME="oscars-test" \
  -e S3_ACCESS_KEY="YOUR_ACCESS_KEY" \
  -e S3_SECRET_KEY="YOUR_SECRET_KEY" \
  -v $(pwd)/data:/data \
  ingestion-pipeline:latest \
  compute_derived_indices \
  --indice "tx30" \
  --temporal-aggregation "monthly" \
  --temporal-aggregation "seasonal" \
  --temporal-aggregation "annual" \
  --overwrite
```

## Notes

- The entrypoint of the Docker image is already set to `pixi run`, so the commands after the image name (`download`, `compute_derived_indices`) are passed directly to the `ingestion-pipeline` CLI.
- Ensure the local `data` directory exists before running the command, or Docker might create it with root permissions.
