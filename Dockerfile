# Dockerfile

FROM python:3.11-slim

# 1) Install Java 17 JDK (needed by Spark) and procps (so 'ps' exists)
RUN apt-get update && \
    apt-get install -y \
      openjdk-17-jdk-headless \
      procps && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# 2) Detect where Java actually lives and write it into a profile script
RUN JAVA_BIN=$(readlink -f "$(which java)") && \
    JAVA_HOME_DIR=$(dirname "$(dirname "$JAVA_BIN")") && \
    echo "export JAVA_HOME=$JAVA_HOME_DIR" > /etc/profile.d/java.sh && \
    echo "export PATH=\$JAVA_HOME/bin:\$PATH" >> /etc/profile.d/java.sh

# 3) Install Python dependencies
RUN pip install --no-cache-dir \
      pyspark \
      pandas \
      pyarrow \
      jupyterlab

# 4) Copy your app code
WORKDIR /app
COPY . /app

# 5) Launch Jupyter under bash login to source /etc/profile.d/java.sh
CMD ["bash", "-lc", "jupyter lab --ip=0.0.0.0 --no-browser --allow-root"]
