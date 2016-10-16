#!/bin/bash
for i in {0..19}
do
    nohup java -cp cassandra-impl-revised-3.jar Processor "d40" "$i.txt" > "d40-20-client-$i.out" &
done