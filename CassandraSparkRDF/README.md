# CassandraSparkRDF
Cassandra and Spark Integration for RDF data. This is a maven-scala project.

## Installation
  ```
  cd CassandraSparkRDF
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
  cassandra_spark.jar \
  /path/to/input \
  /path/to/prefix_file \
  table_name
  ```
  
### Query Executor
  ```
  ./bin/spark-submit \
  --class com.reader.SparkCassandra \
  --master <master-url> \
  --deploy-mode <deploy-mode> \
  --conf <key>=<value> \
  cassandra_spark.jar \
  table_name \
  /path/to/output
  ```
