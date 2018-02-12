#!/bin/bash
hdfs dfs -rm -r /out/RankingByTerms
hadoop jar ./RankingByTerms.jar RankingByTerms /input/sample /out/RankingByTerms/result

# Temporary output between the jobs.
hdfs dfs -cat /out/RankingByTerms/temp/part-r-00000

# Final output.
hdfs dfs -cat /out/RankingByTerms/result/part-r-00000