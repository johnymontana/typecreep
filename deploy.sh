#! /usr/bin/env bash

git pull origin optimize
mvn package
cp target/typecreep-1.0-SNAPSHOT.jar /var/lib/neo4j/plugins/
service neo4j-service restart
