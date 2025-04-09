FROM python:3.12-slim

WORKDIR /app

# Install uv
RUN pip install uv

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