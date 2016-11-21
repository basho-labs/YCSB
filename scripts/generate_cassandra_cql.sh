#!/bin/bash

SIZE="${1:-10}"
COUNT=0
CMD="DROP KEYSPACE ycsb; CREATE KEYSPACE ycsb WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '3'}; CREATE TABLE ycsb.usertable (y_id varchar primary key"

while [ $COUNT -lt $SIZE ]; do
	CMD="$CMD, field$COUNT text"
	let COUNT=COUNT+1
done

CMD="$CMD);"

echo $CMD