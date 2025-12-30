FROM quay.io/strimzi/kafka:0.49.1-kafka-4.1.1
USER root:root
# change ./connect-plugins to path to plugin files
COPY ./connect-plugins/ /opt/kafka/plugins/s3-sink-connector/
USER 1001