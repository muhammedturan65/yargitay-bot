# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Install system dependencies (needed for psycopg2 and others)
# libpq-dev is required for psycopg2
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first to leverage Docker cache
COPY requirements.txt ./

# Install python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code
COPY . .

# Set environment variables (Defaults, can be overridden in Render Dashboard)
ENV PYTHONUNBUFFERED=1
ENV STORAGE_MODE=remote

# Default command: Print help. 
# In Render "Start Command", you will override this with:
# python -m src.uploader --fetch "tapu iptali" --limit 100
CMD ["python", "-m", "src.uploader", "--help"]
