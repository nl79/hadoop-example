#!/bin/bash
javac -classpath $(hadoop classpath) -d . DominantTerms.java
jar cf DominantTerms.jar DominantTerms.class DominantTerms\$DominantTermCountReducer.class DominantTerms\$DominantTermReducer.class DominantTerms\$DominantTermCountMapper.class DominantTerms\$TokenMapper.class