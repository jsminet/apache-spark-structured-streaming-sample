This repository contains Apache Spark structured streaming examples.
Structured streaming is basic streaming combined with Apache Spark SQL
These samples are developed in java code.

First example
-------------
A socket example, launch **nc -lk 50505** before starting

The value first field will be aggregated and counted

Second example
--------------
The rate format is useful only for testing purpose.

It can simulate data flow at the speed you want with this predefined schema

field name| type
---       |---
timestamp | timestamp (nullable = true)
value     | long (nullable = true)

Options:

Name          |Default              |Description
---           |---                  |---
numPartitions |(default parallelism)|Number of partitions to use
rampUpTime    |0 (seconds)          |Time before starting the flow
rowsPerSecond |1                    |Number of rows to generate per second (has to be greater than 0)


Third example 
-------------
### Flow
The third example join polls a HDFS directory which waiting for a csv file with 
students and a score

Then the job join a static csv file to fetch the corresponding score estimation

The highest score is kept into memory and shown into the console

### Getting started
Put *level.csv* static file in the **hdfs://localhost:9000/in/static** directory 
then put the csv file with student score by using the following hdfs command:

```hdfs dfs -put score_yyyyMMdd_hhmmss.csv hdfs://localhost:9000/in/dynamic```

Wait for the job taking the file into consideration