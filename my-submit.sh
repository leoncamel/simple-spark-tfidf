sbt package && MASTER=spark://10.10.0.114:7077 time $SPARK_HOME/bin/spark-submit --class "SimpleApp" target/scala-2.10/simple-project_2.10-1.0.jar
