# Use official lightweight Python image
FROM python:3.11-slim

# System deps (add postgresql-client here)
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq-dev gcc curl postgresql-client \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app/backend

# Copy only requirements first for better layer caching
COPY backend/requirements.txt /app/backend/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the app source (only backend needed at runtime for API)
COPY backend/ /app/backend/

# Non-root user (good practice)
RUN useradd -m appuser && chown -R appuser:appuser /app
USER appuser

# Expose port and default command handled by docker-compose