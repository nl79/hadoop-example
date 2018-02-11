#!/bin/bash
hdfs dfs -rm -r /out/DominantTerms
hadoop jar ./DominantTerms.jar DominantTerms /input/sample /out/DominantTerms/result

# Temporary output between the jobs.
hdfs dfs -cat /out/DominantTerms/temp/part-r-00000

# Final output.
hdfs dfs -cat /out/DominantTerms/result/part-r-00000