#!/bin/bash
hdfs dfs -rm -r /out/RuleMiner
hadoop jar ./RuleMiner.jar RuleMiner /input/transactions /out/RuleMiner/result 1 0.75 21

# Final output.
hdfs dfs -cat /out/RuleMiner/result1/part-r-00000