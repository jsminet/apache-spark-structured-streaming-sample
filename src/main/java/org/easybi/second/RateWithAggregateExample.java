package org.easybi.second;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQueryException;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.date_format;

import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.count;

public class RateWithAggregateExample {

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
                .groupBy(date_format(col("timestamp"), "yy-MM-dd hh:mm")
                 .as("timestamp"))
                .agg(count("value").as("count"))
                .writeStream()
                .format("console")
                // Change output mode to Complete()
                .outputMode(OutputMode.Complete())
                .start()
                .awaitTermination();
    }
}