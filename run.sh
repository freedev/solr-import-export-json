#!/bin/bash

mvn exec:java -Dexec.mainClass="it.damore.solr.backuprestore.App" -Dexec.args="$*"

