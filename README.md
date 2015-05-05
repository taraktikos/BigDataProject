```
export JAVA_HOME=/usr/lib/jvm/java-8-oracle/
export HADOOP_CLASSPATH=out/artifacts/BigDataProject_jar/BigDataProject.jar:./mysql-connector-java-5.1.35-bin.jar
pathtohadoop/bin/hadoop WordCount -libjars mysql-connector-java-5.1.35-bin.jar
pathtohadoop/bin/hadoop MaxTemperature input/ncdc/sample.txt out/ncdc
```