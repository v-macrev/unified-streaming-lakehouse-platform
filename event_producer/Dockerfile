FROM python:3.10-slim

# Prevent Python from writing .pyc files
ENV PYTHONDONTWRITEBYTECODE=1

# Ensure logs appear immediately
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Install system dependencies required by librdkafka
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       gcc \
       librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy dependency manifests
COPY event_producer/requirements.txt ./requirements.txt

# Install runtime dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy applicati
COPY event_producer /app/event_producer
COPY schemas /app/schemas

# Default command
CMD ["python", "-m", "event_producer.src.main"]