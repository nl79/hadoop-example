#!/bin/bash
javac -classpath $(hadoop classpath) -d . *.java
jar cf RuleMiner.jar RuleMiner.class RuleMiner\$FirstPassMapper.class RuleMiner\$kPassMapper.class RuleMiner\$RuleReducer.class Apriori.class Set.class