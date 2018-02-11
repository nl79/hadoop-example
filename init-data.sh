hdfs dfs -mkdir -p /input
hdfs dfs -copyFromLocal data/whole /input/data
hdfs dfs -ls /input/data
