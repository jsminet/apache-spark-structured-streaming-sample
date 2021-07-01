package org.easybi.second;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQueryException;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.date_format;

import java.util.concurrent.TimeoutException;

public class RateExample {

    public static void main(String args[]) throws StreamingQueryException, TimeoutException {

        // Rate format is only for benchmark testing

        SparkSession.builder()
                .master("local")
                .getOrCreate()
                .readStream()
                .format("rate")
                .option("rowsPerSecond", "10")
                .option("rampUpTime", "2")
                .option("numPartitions ", "2")
                .load()
                .withColumn("timestamp",
                        date_format(col("timestamp"), "yy-MM-dd hh:mm:ss"))
                .writeStream()
                .format("console")
                // Same effect with output mode set to Append()
                // Not working with output mode set to Complete()
                .outputMode(OutputMode.Update())
                .start()
                .awaitTermination();
    }
}