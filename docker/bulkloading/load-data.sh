#!/bin/bash

SCRIPT_PATH=`dirname "$0"`

REPO_NAME=$1
DATA_DIR=$2
FORMAT=$3
BASE_URI=$4

MAPPED_DATA_DIR=$SCRIPT_PATH/data
DOCKER_DATA_DIR=/usr/local/tomcat/bulkloading/data
DOCKER_PROPERTIES_FILE=/usr/local/tomcat/bulkload.properties

#Move the data to mapped directory
mkdir -p $MAPPED_DATA_DIR
chmod -R 777 $MAPPED_DATA_DIR

if [[ -d $DATA_DIR ]]; then
	cp -pr $DATA_DIR/* $MAPPED_DATA_DIR
elif [[ -f $DATA_DIR ]]; then
	cp -pr $DATA_DIR $MAPPED_DATA_DIR
else
    echo "$DATA_DIR is not valid"
    exit 1
fi

#Set Heap size
docker exec -it ergo_rdf4j export MAVEN_OPTS="-Xmx1024m -XX:MaxPermSize=128m"

#Ingest the data
docker exec -it -w /usr/local/tomcat/IngestionPipeline/GraphIngestion ergo_rdf4j  mvn exec:java -Dexec.mainClass="com.ibm.research.ergs.ingestion.loader.BulkLoader" -Dexec.args="$REPO_NAME $DOCKER_DATA_DIR $FORMAT $BASE_URI $DOCKER_PROPERTIES_FILE" -Dexec.cleanupDaemonThreads=false

#delete the data from mapped directory
rm -r $MAPPED_DATA_DIR/*



