# Use official Python runtime as base image
FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY config.py .
COPY rabbitmq_client.py .
COPY s3_client.py .
COPY input_validator.py .
COPY data_copy_service.py .
COPY cloudwatch_utils.py .
COPY main.py .

# Copy Snowflake module (assuming it exists in the build context)
COPY snowflake_external_table.py .

# --- ADD THIS LINE ---
# Copy the analytics query SQL file
COPY analytics_wastage_queries.sql /app/analytics_wastage_queries.sql

# Create non-root user for security
RUN groupadd -r appuser && useradd -r -g appuser appuser
RUN chown -R appuser:appuser /app
USER appuser

# Set the entrypoint
ENTRYPOINT ["python", "main.py"]