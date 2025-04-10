FROM python:3.12-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      openjdk-17-jdk-headless \
      ca-certificates \
      curl \
      gnupg \
      unzip \
      procps \
      bash && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install uv
RUN pip install uv

# Copy spark db drivers
COPY drivers ./drivers

# Copy pyproject.toml and poetry.lock or pdm.lock
COPY pyproject.toml uv.lock .

# Install dependencies using uv
RUN uv pip install --system --no-cache-dir .

# Copy the source code
COPY src ./src
COPY scripts ./scripts

# Set the PYTHONPATH to include the src directory
ENV PYTHONPATH=src

# Define the command to run your application
CMD ["uvicorn", "src.kpi_api:app", "--host", "0.0.0.0", "--port", "8000"]