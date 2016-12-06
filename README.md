### solr-backup-restore-json

# Backup or restore a Solr collection to/from a json file.

enjoy :)


To execute this console app you need to satisfy few dependency (java 8, git, maven), if you are a java developer probably you already have everything, on the other hand if not if you have Linux execute the following commands:

```
  sudo apt-get update
  sudo add-apt-repository ppa:openjdk-r/ppa
  sudo apt-get update
  sudo apt-get install openjdk-8-jdk
  sudo apt-get install maven
  
  git clone https://github.com/freedev/solr-backup-restore-json.git
  cd solr-backup-restore-json
  mvn clean package
```

Now you're ready to execute this little script.

- This is the list of command line parameters:

```
	usage: ./run.sh [-a <arg>] [-d] [-D] [-f <arg>] [-h] [-k <arg>] [-o <arg>]
	       [-s <arg>] [-S <arg>]
	solr-backup-restore-json
	
	 -a,--actionType <arg>    action type [backup|restore]
	 -d,--deleteAll           delete all documents before restore
	 -D,--dryRun              dry run test
	 -f,--filterQuery <arg>   filter Query during backup
	 -h,--help                help
	 -k,--uniqueKey <arg>     specify unique key for deep paging
	 -o,--output <arg>        output file
	 -s,--solrUrl <arg>       solr url
	 -S,--skipFields <arg>    comma separated fields list to skip during
	                          backup/restore
```

Here few real examples:

- backup all documents into a json file

```
#!bash
  ./run.sh -s http://localhost:8983/solr/collection -a backup -o /tmp/collection.json

```

- restore documents from json

```
#!bash
  ./run.sh -s http://localhost:8983/solr/collection -a restore -o /tmp/collection.json 

```

- backup filtering documents, same as fq Solr parameter

```
#!bash
  ./run.sh -s http://localhost:8983/solr/collection -a backup -o /tmp/collection.json --filterQuery field:value

```

- restore documents from json but first delete all documents

```
#!bash
  ./run.sh -s http://localhost:8983/solr/collection -a restore -o /tmp/collection.json --deleteAll

```
