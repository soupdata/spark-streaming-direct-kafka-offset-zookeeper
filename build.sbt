name := "sparkproject"

version := "0.1"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "2.2.1"

libraryDependencies += "org.apache.spark" % "spark-mllib_2.10" % "2.2.1"

libraryDependencies += "org.apache.spark" % "spark-streaming_2.10" % "2.2.1"

libraryDependencies += "org.apache.spark" % "spark-sql_2.10" % "2.2.1"

libraryDependencies += "org.apache.spark" % "spark-graphx_2.10" % "2.2.1"

// https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-common
libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.7.3" % "provided"

// https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-hdfs
libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs" % "2.7.3"

// https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-client
libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.7.3"

//// https://mvnrepository.com/artifact/org.apache.kafka/kafka
//libraryDependencies += "org.apache.kafka" %% "kafka" % "0.10.2.1"

// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming-kafka
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka" % "1.6.3"

// https://mvnrepository.com/artifact/org.apache.kafka/kafka-clients
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "2.1.0"

//
//// https://mvnrepository.com/artifact/org.apache.kafka/kafka-streams
//libraryDependencies += "org.apache.kafka" % "kafka-streams" % "0.10.2.1"
//// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming-kafka
//libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka" % "1.3.0"


