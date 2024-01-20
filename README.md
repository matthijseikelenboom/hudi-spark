# Hudi Spark Example

This project contains some sample code on how to setup Apache Hudi in combination with Apache Spark and Apache Hive.
It's made and tested in macOS 14 Sonoma.

## Component version
- Apache Hudi 0.14.1
- Apache Spark 3.4.2
- Apache Hive 4.0.0-beta-1

## Running the tests
To run the tests it is required to first have a Hive service running.
The `HoodieSparkSession` class can spin up a Hive metastore container for you or you can sping one up yourself.
This can be done by using the following command: `docker run --rm -d -p 9083:9083 --env SERVICE_NAME=metastore --name hive4metastore apache/hive:4.0.0-beta-1`
