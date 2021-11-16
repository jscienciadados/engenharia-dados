spark-submit --class org.apache.spark.examples.SparkPi \
    --master yarn \
    --deploy-mode cluster \
    --driver-memory 3g \
    --executor-memory 2g \
    --executor-cores 1 \
    /opt/spark/examples/jars/spark-examples*.jar \
    10
