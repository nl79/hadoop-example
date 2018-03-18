hdfs dfsadmin -safemode leave
hdfs dfs -rm -r /input
hdfs dfs -mkdir -p /input/transactions

hdfs dfs -copyFromLocal ../../data/transactions/full /input/transactions/full
hdfs dfs -ls /input/transactions/full

hdfs dfs -copyFromLocal ../../data/transactions/sample /input/transactions/sample
hdfs dfs -ls /input/transactions/sample