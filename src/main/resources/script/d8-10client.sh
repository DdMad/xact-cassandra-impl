#!/bin/bash
for i in {0..9}
do
    nohup java -cp cassandra-impl-revised-3.jar Processor "d8" "$i.txt" > "d8-10-client-$i.out" &
done