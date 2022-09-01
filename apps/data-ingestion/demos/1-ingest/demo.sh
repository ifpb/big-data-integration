cd ../../data-ingestion/
docker-compose up -d
sleep 10
python ingest.py mssql
python ingest.py mysql
python ingest.py postgres
python ingest.py minio

spark-submit \
--conf spark.hadoop.fs.s3a.endpoint=http://localhost:9000 \
--conf spark.hadoop.fs.s3a.access.key=YOURACCESSKEY \
--conf spark.hadoop.fs.s3a.secret.key=YOURSECRETKEY \
--conf spark.hadoop.fs.s3a.path.style.access=True \
--conf spark.hadoop.fs.s3a.fast.upload=True \
--conf spark.hadoop.fs.s3a.connection.maximum=100 \
--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
--conf spark.delta.logStore.class=org.apache.spark.sql.delta.storage.S3SingleDriverLogStore \
--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
/Users/luanmorenomaciel/BitBucket/airflow/spark/pr-elt-business/pr-elt-business.py