# Use official Python runtime as base image
FROM python:3.11-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Install system dependencies
RUN apt-get update && apt-get install -y \
    # PostgreSQL client and libraries
    postgresql-client \
    libpq-dev \
    # C/C++ development tools
    gcc \
    g++ \
    make \
    cmake \
    build-essential \
    # Networking tools
    iputils-ping \
    # Clean up
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements file (if you have one)
# COPY requirements.txt .

# Install Python PostgreSQL libraries
RUN pip install --no-cache-dir \
    psycopg2-binary \
    psycopg2 \
    SQLAlchemy \
    alembic

RUN pip install paho-mqtt
RUN pip install nats-py

# Uncomment to install from requirements.txt
# RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
# COPY . .

# Create a non-root user to run the application
RUN useradd -m -u 1000 appuser && chown -R appuser:appuser /app
USER appuser

# Default command (can be overridden)
CMD ["bash"]
