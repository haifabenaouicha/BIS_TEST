spark-submit \
  --master local[*] \
  --conf "spark.app.name=BISApp" \
  --conf "spark.sql.debug.maxToStringFields=1000" \
   --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
  --conf "spark.sql.adaptive.enabled=true" \
  --conf "spark.sql.adaptive.coalescePartitions.enabled=true" \
  --conf "spark.sql.adaptive.skewJoin.enabled=true" \
  --conf "spark.sql.autoBroadcastJoinThreshold=104857600" \
  --conf "spark.sql.broadcastTimeout=120" \
  --conf "spark.sql.shuffle.partitions=200" \
  --conf "spark.default.parallelism=200" \
  --conf "spark.sql.inMemoryColumnarStorage.compressed=true" \
  --conf "spark.sql.inMemoryColumnarStorage.batchSize=10000" \
  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
  --packages io.delta:delta-core_2.12:2.4.0 \
  --py-files /Users/haifabenaouicha/Documents/BIS_TEST/dist/bis_test-1.0.0-py3-none-any.whl \
  main.py --job dim --conf conf/job.conf

