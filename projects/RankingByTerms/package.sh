#!/bin/bash
javac -classpath $(hadoop classpath) -d . RankingByTerms.java
jar cf RankingByTerms.jar RankingByTerms.class RankingByTerms\$RankToStateReducer.class RankingByTerms\$RankToStateMapper.class RankingByTerms\$TokenRankReducer.class RankingByTerms\$TokenMapper.class RankingByTerms\$TokenRankReducer\$1.class