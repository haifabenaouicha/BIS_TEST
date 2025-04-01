spark-submit \
  --master local[*] \
  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
  --packages io.delta:delta-core_2.12:2.4.0 \
  main.py conf/job.conf
