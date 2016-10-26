# solr-backup-restore-json

This is a simple command line application able to backup/restore a solr collection to/from a json file.

enjoy :)

- environment dependency: java 8 e maven


With linux execute:
```
#!bash

  sudo apt-get update
  sudo apt-get remove openjdk-7-jdk
  sudo add-apt-repository ppa:openjdk-r/ppa
  sudo apt-get update
  sudo apt-cache search java | grep 8
  sudo apt-get install openjdk-8-jdk
  sudo apt-get install maven
  
  mvn clean package

```

- build 


```
#!bash
  mvn clean package

```

- help

  ./run.sh -h

- backup 

```
#!bash
  ./run.sh -s http://localhost:8983/solr/collection -a backup -f /tmp/collection.json

```

- restore parameters

```
#!bash
  ./run.sh -s http://localhost:8983/solr/collection -a restore -f /tmp/collection.json

```
