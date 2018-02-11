hdfs dfs -mkdir -p /input
hdfs dfs -copyFromLocal data/sample /input/sample
hdfs dfs -ls /input/sample