package org.easybi.third;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.max;

import java.util.concurrent.TimeoutException;

public class StaticJoinDynamicAggregation {

    public static void main(String args[]) throws StreamingQueryException, TimeoutException {

        String hdfsURL = "hdfs://localhost:9000";

        SparkSession sparkSession = SparkSession
                .builder()
                .master("local[*]")
                .getOrCreate();

       /* level.csv
        score | description
        0     | Dumbass
        1     | Back to school
        ...
        19    | Amazing
        20    | Perfect */

        Dataset<Row> staticDf = sparkSession
                .read()
                .format("csv")
                // WARNING: You can infer ONLY with static dataframe
                .option("inferSchema","true")
                .option("header","true")
                .option("path", hdfsURL.concat("/in/static/level.csv"))
                .load();

        // WARNING: schema MUST be defined with dynamic dataframe
        StructType struct = new StructType()
                .add("name", "string")
                .add("score","integer");

        /* score_yyyyMMdd_hhmmss.csv
        name     | score
        Toto     | 5
        Tata     | 10
        Tutu     | 15
         */
        Dataset<Row> dynamicDf = sparkSession
                .readStream()
                .format("csv")
                // Put csv file here by using "hdfs dfs -put" command
                // This is the polling directory
                .option("path", hdfsURL.concat("/in/dynamic"))
                .schema(struct)
                .load();

        Dataset<Row> df2 = dynamicDf
                .groupBy("name")
                .agg(max("score").as("score"));

        df2.join(staticDf,"score")
                .writeStream()
                .format("console")
                .outputMode(OutputMode.Update())
                .start()
                .awaitTermination();
    }
}