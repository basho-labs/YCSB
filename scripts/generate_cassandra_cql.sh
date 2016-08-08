#!/bin/bash

SIZE="${1:-10}"
COUNT=0
CMD="DROP KEYSPACE ycsb; CREATE KEYSPACE ycsb WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}; CREATE TABLE ycsb.usertable (time timestamp, family text, series text"

while [ $COUNT -lt $SIZE ]; do
	CMD="$CMD, field$COUNT text"
	let COUNT=COUNT+1
done

CMD="$CMD, PRIMARY KEY ((family, series), time));"

echo $CMD