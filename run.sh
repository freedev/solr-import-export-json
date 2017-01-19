#!/bin/bash

PARAMS="$*"

if [ "A$PARAMS" == "A" ]
then
  PARAMS="--help"
fi

mvn exec:java -Dexec.mainClass="it.damore.solr.importexport.App" -Dexec.args="$PARAMS"

