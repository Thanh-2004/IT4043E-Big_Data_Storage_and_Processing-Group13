mkdir -p /tmp/.ivy2 && /opt/spark/bin/spark-submit \
    --master local \
    --conf spark.jars.ivy=/tmp/.ivy2 \
    --conf spark.executor.cores=2 \
    --conf spark.executor.memory=2g \
    --conf spark.executor.memoryOverhead=512m \
    --conf spark.cores.max=4 \
    --conf spark.dynamicAllocation.enabled=false \
    --conf spark.executor.instances=1 \
    --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.0,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.104.5,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
    /opt/spark/jobs/batch_processing_demo.py