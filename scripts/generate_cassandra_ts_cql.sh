#!/bin/bash

SIZE="${1:-10}"
COUNT=0
CMD="DROP KEYSPACE ycsb; CREATE KEYSPACE ycsb WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}; CREATE TABLE ycsb.usertable (host varchar, worker varchar, quanta timestamp, time timestamp"

while [ $COUNT -lt $SIZE ]; do
	CMD="$CMD, field$COUNT text"
	let COUNT=COUNT+1
done

CMD="$CMD, PRIMARY KEY ((quanta, host, worker), time));"

echo $CMD