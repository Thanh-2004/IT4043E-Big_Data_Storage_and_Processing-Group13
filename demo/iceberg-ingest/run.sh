mkdir -p /tmp/.ivy2 && /opt/spark/bin/spark-submit \
    --master local[*] \
    --conf spark.jars.ivy=/tmp/.ivy2 \
    --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.0,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.104.5,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
    /app/main.py