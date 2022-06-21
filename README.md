### solr-import-export-json

# Import/Export (or Restore/Backup) a Solr collection from/to a json file.

As the title states, this little project will help you to save your collection in json format and restore it where and when you need.

Please report issues at https://github.com/freedev/solr-import-export-json/issues

### Install

To execute this console app you need to satisfy few dependency (java 11, git, maven), if you are a java developer probably you already have everything, on the other hand if not if you have Linux execute the following commands:

    sudo apt update
    sudo apt install git openjdk-11-jdk maven
  
    git clone https://github.com/freedev/solr-import-export-json.git
    cd solr-import-export-json
    mvn clean package

Now you're ready.

### How to use it

This is the list of command line parameters.

    usage: myapp [-a <arg>] [-b <arg>] [-C] [-c <arg>] [-d] [-D] [-f <arg>]
           [-F <arg>] [-h] [-i <arg>] [-k <arg>] [-o <arg>] [-p <arg>] [-s
           <arg>] [-S <arg>] [-u <arg>] [-x <arg>]
    solr-import-export-json
    
     -a,--actionType <arg>           action type
                                     [import|export|backup|restore]
     -b,--blockSize <arg>            block size (default 5000 documents)
     -C,--disableCursors             disable Solr cursors while reading
     -c,--commitDuringImport <arg>   Commit progress after specified number of
                                     docs. If not specified, whole work will
                                     be committed.
     -d,--deleteAll                  delete all documents before import
     -D,--dryRun                     dry run test
     -f,--filterQuery <arg>          filter Query during export
     -F,--dateTimeFormat <arg>       set custom DateTime format (default
                                     yyyy-MM-dd'T'HH:mm:ss.SSS'Z')
     -h,--help                       help
     -i,--includeFields <arg>        simple comma separated fields list to be
                                     used during export. if not specified all
                                     the existing fields are used
     -k,--uniqueKey <arg>            specify unique key for deep paging
     -o,--output <arg>               output file
     -p,--password <arg>             basic auth password
     -s,--solrUrl <arg>              solr url -
                                     http://localhost:8983/solr/collection_nam
                                     e
     -S,--skipFields <arg>           comma separated fields list to skip
                                     during export/import, this field list
                                     accepts for each field prefix/suffix a
                                     wildcard *. So you can specify skip all
                                     fields starting with name_*
     -u,--user <arg>                 basic auth username
     -x,--skipCount <arg>            Number of documents to be skipped when
                                     loading from file. Useful when an error
                                     occurs, so loading can continue from last
                                     successful save.

### Real life examples

export all documents into a json file

    ./run.sh -s http://localhost:8983/solr/collection -a export -o /tmp/collection.json

import documents from json

    ./run.sh -s http://localhost:8983/solr/collection -a import -o /tmp/collection.json 

export part of documents, like adding a `fq`  Solr parameter to the export

     ./run.sh -s http://localhost:8983/solr/collection -a export -o /tmp/collection.json --filterQuery field:value

import documents from json but first delete all documents in the collection

     ./run.sh -s http://localhost:8983/solr/collection -a import -o /tmp/collection.json --deleteAll

export documents and skip few fields. In the example the will be skipped the fields: `field1_a`, all the fields starting with `field2_` and all the fields ending with `_date`

     ./run.sh -s http://localhost:8983/solr/collection -a export -o /tmp/collection.json --skipFields field1_a,field2_*,*_date

Import documents, skip first 49835000 records from file, commit every 200000 documents, block size 5000 (faster than default 500) 

    ./run.sh -s http://localhost:8983/solr/collection -a import -o /tmp/collection.json -x 49835000 -c 200000 -b 5000 
