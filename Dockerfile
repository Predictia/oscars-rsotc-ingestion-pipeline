FROM ghcr.io/prefix-dev/pixi:latest

# Set work directory
WORKDIR /workdir

# Copying files from the repository
COPY ingestion_pipeline ingestion_pipeline
COPY pyproject.toml pyproject.toml
COPY pixi.toml pixi.toml
COPY pixi.lock pixi.lock
COPY README.md README.md
COPY LICENSE LICENSE
COPY templates templates
COPY scripts/provenance/render_static_config.py scripts/provenance/render_static_config.py

USER root
RUN apt-get update -y && \
    apt-get install -y g++ curl && \
    rm -rf /var/lib/apt/lists/*

# Install dependencies and set up environment
RUN pixi install && pixi clean

# Install the package
RUN pixi run python -m pip install --no-cache-dir --no-deps .

# Use pixi run as default entrypoint
ENTRYPOINT ["pixi", "run"]
