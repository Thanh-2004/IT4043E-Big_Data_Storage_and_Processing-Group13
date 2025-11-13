mkdir -p /tmp/.ivy2 && /opt/spark/bin/spark-submit \
    --master local[*] \
    --packages \
    org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
    --conf spark.jars.ivy=/tmp/.ivy2 \
    /app/main.py