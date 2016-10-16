# xact-cassandra-impl
Trade transaction system implemented by cassandra

## Setup
Run `gradle` build to download dependencies

Or you can choose to use the compiled executable jar file

## Load Data
After you set up with Intellij or some toher IDE, you can run `Loader` class to load data into database. In additon, you can configure the path of the `schema` file and the `data` file inside the class.

In addition, you can just run following command:
### Load D8 database:
```bash
java -cp cassandra-impl-revised-3.jar Loader "d8"
```
### Load D40 database:
```bash
java -cp cassandra-impl-revised-3.jar Loader "d40"
```
The default schema file path and file name is `resources/setup-d8.cql` and `resources/setup-d40.cql`;
The default data file directory is `resources/D8-data/` and `resources/D40-data/`, and all the corresponding csv files will be loaded into the database.

## Run Transaction
After loading data, you can run one client using following command:

### Run D8 Transactions
```bash
java -cp cassandra-impl-revised-3.jar Processor "d8" "0.txt"
```
### Run D40 Transactions
```bash
java -cp cassandra-impl-revised-3.jar Processor "d40" "0.txt"
```

The text file `0.txt` can be changed to other transaction files.

Or you may want to modify the Java class. After your modification, you can compile the whole project to a executable jar file and then run it.

### Run Multiple Client Simultaneously
If you want to run multiple client at the same time, we have provide some bash script which are in the `resouces/script`. Put it within the same folder as the executable jar file and then run it.

## Output
All the transaction result will be written to a file. For example, for transaction file `0.txt`, the default output is `0.txt-out.txt`. You may want to change this format in the XactProcessor.java.

The final result of thoughput will be written in a file called `result-d8-n.txt.out` or `result-d40-n.txt.out`, where `n` is the number of transaction file.

## PS
When you run the jar, ensure that there is a `log4j.properities`. If you run the jar in other places, just copy the `log4j.properities` in this project to the same place.

