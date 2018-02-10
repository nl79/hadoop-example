hdfs dfs -rm -r /out/WordCount
hadoop jar ../projects/WordCount/WordCount.jar WordCount /input/data /out/WordCount

# hdfs dfs -cat /out/WordCount/part-r-00000
