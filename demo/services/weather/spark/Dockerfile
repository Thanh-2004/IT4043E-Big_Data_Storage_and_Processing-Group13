FROM eclipse-temurin:17-jdk

RUN apt-get update && \
    apt-get install -y python3 python3-pip wget && \
    ln -s /usr/bin/python3 /usr/bin/python

ENV SPARK_VERSION=3.4.2
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

RUN mkdir -p /opt && \
    cd /opt && \
    wget --no-verbose https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
    tar -xzf spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop3 ${SPARK_HOME} && \
    rm spark-${SPARK_VERSION}-bin-hadoop3.tgz

RUN pip install pyspark==${SPARK_VERSION} pymongo

WORKDIR /opt/spark
