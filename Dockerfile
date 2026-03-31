FROM python:3.11-slim AS base

WORKDIR /app

# Install system dependencies for geopandas (optional)
RUN apt-get update && apt-get install -y --no-install-recommends \
    libgdal-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Default data directory
ENV DATA_DIR=/data
VOLUME /data

ENTRYPOINT ["python", "cli.py"]
CMD ["status"]
