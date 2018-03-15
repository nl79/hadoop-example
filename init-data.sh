hdfs dfs -mkdir -p /input
hdfs dfs -copyFromLocal data/whole /input/data
hdfs dfs -copyFromLocal data/transactions /input/transactions
hdfs dfs -ls /input/data
hdfs dfs -ls /input/transactions
