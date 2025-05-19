# Dockerfile

# Use an official Python image with OpenJDK
FROM python:3.11-slim

# Install Java (needed for PySpark)
RUN apt-get update && \
    apt-get install -y openjdk-8-jdk-headless && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME for PySpark
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"

# Install system deps and Python packages
RUN pip install --no-cache-dir pyspark pandas pyarrow jupyterlab

# Create app directory
WORKDIR /app

# Copy your code & data definitions (Bronze/Silver/Gold folders will be volume-mounted)
COPY . /app

# Default to Jupyter Lab, but you can override with `docker-compose run`
CMD ["jupyter", "lab", "--ip=0.0.0.0", "--no-browser", "--allow-root"]
