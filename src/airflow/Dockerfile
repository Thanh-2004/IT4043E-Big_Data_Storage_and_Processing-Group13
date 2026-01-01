FROM apache/airflow:2.10.4

USER root

# 1. Install Java and procps (for 'ps' command)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    procps \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# 2. Automatically detect Java path (Works on both M1 Mac and Intel)
# We create a universal symlink at /opt/java-17
RUN ln -s /usr/lib/jvm/java-17-openjdk-$(dpkg --print-architecture) /opt/java-17

# 3. Set JAVA_HOME to the symlink
ENV JAVA_HOME=/opt/java-17

USER airflow

# 4. Install PySpark and Providers
RUN pip install --no-cache-dir \
    apache-airflow-providers-apache-spark==4.7.0 \
    pyspark==3.5.0