#!/bin/bash

mvn exec:java -Dexec.mainClass="it.damore.solr.backuprestore.App" -Dexec.args=" -solrUrl ${1} -a ${2:backup} -f ${3}"

