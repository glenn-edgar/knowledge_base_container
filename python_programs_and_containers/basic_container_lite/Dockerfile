# Use Python 3.11 slim image as base
FROM nanodatacenter/python_lite:latest
# Set working directory
WORKDIR /app
# Switch to root user to install packages
USER root
# Install system dependencies in a single layer
RUN apt-get update && apt-get install -y \
    curl \
    tree \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
# Switch back to non-root user if the base image had one
# USER appuser
# Copy requirements first for better Docker layer caching
COPY requirements.txt .
# Install Python dependencies including Docker SDK and MQTT libraries
RUN pip install --no-cache-dir -r requirements.txt && \
    pip install --no-cache-dir paho-mqtt asyncio-mqtt
# Copy the Python application files
COPY . .
# Command to run the application
CMD ["bash"]

