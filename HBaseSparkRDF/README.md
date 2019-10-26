# HBaseSparkRDF
HBase and Spark Integration for RDF data. This is a maven-scala project.

## Installation
  ```
  cd HBaseSparkRDF
  mvn package
  ```
## Execution
### RDF Loader
  ```
  ./bin/spark-submit \
  --class com.loader.Driver \
  --master <master-url> \
  --deploy-mode <deploy-mode> \
  --conf <key>=<value> \
  hbase_spark.jar \
  /path/to/input \
  /path/to/prefix_file \
  table_name
  ```
  
### Query Executor
  ```
  ./bin/spark-submit \
  --class com.reader.SparkHBase \
  --master <master-url> \
  --deploy-mode <deploy-mode> \
  --conf <key>=<value> \
  hbase_spark.jar \
  table_name \
  zooKeeper_quorum \
  /path/to/output
  ```
