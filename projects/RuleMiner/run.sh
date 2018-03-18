#!/bin/bash
iteration=3

hdfs dfs -rm -r /out/RuleMiner
hadoop jar ./RuleMiner.jar RuleMiner /input/transactions/full /out/RuleMiner/result ${iteration} 0.3 20

# Final output.
hdfs dfs -cat /out/RuleMiner/result-${iteration}/part-r-00000

# Final output.
# hdfs dfs -cat /out/RuleMiner/result-1/part-r-00000