### solr-import-export-json

# Import/Export (or Restore/Backup) a Solr collection from/to a json file.

enjoy :)


To execute this console app you need to satisfy few dependency (java 8, git, maven), if you are a java developer probably you already have everything, on the other hand if not if you have Linux execute the following commands:

    sudo apt-get update
    sudo add-apt-repository ppa:openjdk-r/ppa
    sudo apt-get update
    sudo apt-get install openjdk-8-jdk
    sudo apt-get install maven
  
    git clone https://github.com/freedev/solr-import-export-json.git
    cd solr-import-export-json
    mvn clean package

Now you're ready to execute this little script.

- This is the list of command line parameters:

    usage: myapp [-a <arg>] [-d] [-D] [-f <arg>] [-h] [-k <arg>] [-o <arg>]
       [-s <arg>] [-S <arg>]
    solr-import-export-json

    -a,--actionType <arg>    action type [import|export|backup|restore]
    -d,--deleteAll           delete all documents before import
    -D,--dryRun              dry run test
    -f,--filterQuery <arg>   filter Query during export
    -h,--help                help
    -k,--uniqueKey <arg>     specify unique key for deep paging
    -o,--output <arg>        output file
    -s,--solrUrl <arg>       solr url
    -S,--skipFields <arg>    comma separated fields list to skip during
                             export/import, this field accepts start and end
                             wildcard *. So you can specify skip all fields
                             starting with name_*

Please report issues at https://github.com/freedev/solr-import-export-json

Here few real examples:

- export all documents into a json file

    #!bash
    ./run.sh -s http://localhost:8983/solr/collection -a export -o /tmp/collection.json

- import documents from json

     #!bash
    ./run.sh -s http://localhost:8983/solr/collection -a import -o /tmp/collection.json 

- export part of documents, like adding a `fq`  Solr parameter to the export

     #!bash
     ./run.sh -s http://localhost:8983/solr/collection -a export -o /tmp/collection.json --filterQuery field:value

- import documents from json but first delete all documents in the collection

     #!bash
     ./run.sh -s http://localhost:8983/solr/collection -a import -o /tmp/collection.json --deleteAll

- export the documents and skip few fields. In the example the will be skipped the fields: `field1_a`, all the fields starting with `field2_` and all the fields ending with `_date`

     #!bash
     ./run.sh -s http://localhost:8983/solr/collection -a export -o /tmp/collection.json --skipFields field1_a,field2_*,*_date
    