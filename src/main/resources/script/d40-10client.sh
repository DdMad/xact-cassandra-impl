#!/bin/bash
for i in {0..9}
do
    nohup java -cp cassandra-impl.jar Processor "d40" "$i.txt" > "d40-10-client-$i.out" &
done