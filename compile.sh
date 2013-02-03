export HADOOP_HOME=/usr/class/cs149/hadoop-0.21.0

javac –cp ${HADOOP_HOME}/hadoop-common-0.21.0.jar:${HADOOP_HOME}/hadoop-hdfs-0.21.0.jar:${HADOOP_HOME}/hadoop-mapred-0.21.0.
jar –dclass_dir Tokenizer.java Ngram.java
 jar –cvf ngram.jar –C class_dir/ export HADOOP_HOME=/usr/class/cs149/hadoop-0.21.0

