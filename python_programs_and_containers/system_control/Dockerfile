# Use Python 3.11 slim image as base
FROM nanodatacenter/python_base_image

# Set working directory
WORKDIR /app

# Install system dependencies in a single layer
RUN apt-get update && apt-get install -y \
    gcc \
    python3-dev \
    libpq-dev \
    curl \
    tree \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

RUN apt update && apt install -y postgresql-client
# Copy requirements first for better Docker layer caching
COPY requirements.txt .

# Install Python dependencies including Docker SDK
RUN pip install --no-cache-dir -r requirements.txt && \
    pip install --no-cache-dir docker

# Copy the Python application files
COPY . .

# Command to run the application
CMD ["python", "startsup.py"]
