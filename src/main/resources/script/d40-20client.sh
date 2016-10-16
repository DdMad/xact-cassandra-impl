#!/bin/bash
for i in {0..19}
do
    nohup java -cp cassandra-impl.jar Processor "d40" "$i.txt" > "d40-20-client-$i.out" &
done