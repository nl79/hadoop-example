hdfs dfs -mkdir -p /input/transactions
hdfs dfs -copyFromLocal data/whole /input/data
hdfs dfs -copyFromLocal data/transactions/full /input/transactions/full
hdfs dfs -ls /input/data
hdfs dfs -ls /input/transactions/full
