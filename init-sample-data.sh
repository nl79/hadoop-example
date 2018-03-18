hdfs dfsadmin -safemode leave
hdfs dfs -rm -r /input
hdfs dfs -mkdir -p /input

hdfs dfs -copyFromLocal data/sample /input/sample
hdfs dfs -copyFromLocal data/transactions/sample /input/transactions/sample
hdfs dfs -ls /input/sample
hdfs dfs -ls /input/transactions/sample