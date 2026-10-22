# ============================================================
# Dockerfile for BDA Review 3 — Apache Spark & PySpark Analytics
# NYC Yellow Taxi Demand & Fare Intelligence System
# ============================================================
FROM python:3.10-slim

ENV DEBIAN_FRONTEND=noninteractive

# Install OpenJDK 17, procps, curl, and build tools
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jdk-headless \
    procps \
    curl \
    bash \
    && rm -rf /var/lib/apt/lists/*

# Configure Java environment
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

WORKDIR /app

# Install Python dependencies
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copy Review 3 pipeline files
COPY . /app/review3/
WORKDIR /app/review3
RUN chmod +x run_all.sh

# Run the complete pipeline by default
CMD ["bash", "run_all.sh"]
