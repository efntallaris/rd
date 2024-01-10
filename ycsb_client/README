# Redis Data Validator

This a data validator for the Redis database (https://redis.io).
It can be used as a sanity checker for the data integrity stored in a Redis cluster.
The tool is primarily intended for use by Redis-internals developers but is also useful to perform an sanity check scan on the database after cluster administrative operations (e.g. restarts, elasticity actions).

The data validator discovers Redis Cluster metadata (nodes, key distribution across nodes, etc). It reads data from file and then probes Redis Cluster nodes to check is requests are served by the nodes as expected and data are not corrupted.


## Prerequisites

This is a Java project that has the following dependencies 

| Library				| Version |
| :----:				| :----:  |
| Lettuce				| 5.4	  |
| Apache commons CLI	| 1.3.1   |
| Guava					| 31.1	  | 

## Build

The project can be build using the maven project management tool which will automatically download and install all the necessary library dependencies with the following command.

mvn clean package

This build process will compile and pack the application in a jar file located in the target directory

### Execution
You can use the tool using the produced jar file from the build process.
From the root directory execute:

java -jar ./target/data_validator-0.1.jar <parameters>

There are some required and optional parameters. The following table summarizes the command line parameters.

| Short name	| Long name	| Expected Value	| Required	| Default Value| Description |
|   :----:		|    :----:	|	  :----:		| :----:	|    :----:	   |			 |
|	-d			| --datafile|		String		|	Yes		|	  ---	   | The input data file|
|	-h			| --host	|	String			|	No		| 127.0.0.1	   | The target Redis Cluster node IP address |
|	-p			| --port	|	int				|	No		|	6379	   | The target Redis Cluster node port number|

An example of the execution command line

java -jar ./target/data_validator-0.1.jar -h 1.2.3.4 -p 6380 -d sampleData.dat


