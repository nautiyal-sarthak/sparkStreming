# sparkStreming


spark-submit --class com.cn.otds.snow.SnowStreamingApps --master yarn  --deploy-mode cluster  --queue otds_comn --jars $KAFKA_HOME/libs/kafka-clients-2.0.0.3.1.0.0-78.jar,$KAFKA_HOME/libs/kafka_2.11-2.0.0.3.1.0.0-78.jar,$KAFKA_HOME/libs/metrics-core-2.2.0.jar,$KAFKA_HOME/libs/zkclient-0.10.jar,/home/193463/spark-sql-kafka-0-10_2.11-2.3.2.3.1.0.0-78.jar,/home/193463/kafka-clients-2.7.0.jar --files /home/193463/client_jaas.conf,/home/193463/.193463.keytab --conf "spark.driver.extraJavaOptions=$JAVAENV_OPTION_CNTRL" --conf "spark.executor.extraJavaOptions=$JAVAENV_OPTION_CNTRL"  SnowStreamingApps-1.0.0-SNAPSHOT.jar DEV


ENV Var : 
-Djavax.security.auth.useSubjectCredsOnly=false -Djavax.net.ssl.trustStore=C:\Apps\truststore.jks
