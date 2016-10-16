#!/bin/bash
for i in {0..39}
do
    nohup java -cp cassandra-impl-revised-3.jar Processor "d8" "$i.txt" > "d8-40-client-$i.out" &
done