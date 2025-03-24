# Stage 1: Build Stage
FROM python:3.10-slim-bookworm AS build

WORKDIR /src

ENV PYTHONUNBUFFERED=1

# Add requirements and install dependencies
ADD ./main_app/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Stage 2: Final Stage (Production)
FROM python:3.10-slim-bookworm AS final

# Create a non-root user and group, and ensure home directory exists
RUN groupadd -r appuser && useradd -r -g appuser -d /home/appuser -m appuser

# Copy only the necessary files from the build stage
COPY --from=build /usr/local/lib/python3.10/site-packages /usr/local/lib/python3.10/site-packages
COPY --from=build /usr/local/bin/ /usr/local/bin/

# Install required system dependencies
RUN apt-get update && apt-get install -y libmagic1 libmagic-dev vim curl

# Install system dependencies
RUN apt-get update && apt-get install -y \
    libsndfile1 \
    && rm -rf /var/lib/apt/lists/*

# Switch to the non-root user
USER appuser

WORKDIR /src

# Bring the build-time ARGs into the final stage
ARG BUILD_ENV
ARG LOG_LEVEL

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV BUILD_ENV=${BUILD_ENV}
ENV LOG_LEVEL=${LOG_LEVEL}

# Add application code (ensure you use --chown to assign proper ownership)
COPY --chown=appuser:appuser main_app /src/main_app

# # Run the main application (update this line with your entry point)
# CMD ["python", "app_base/config/create_external_config.py"]
