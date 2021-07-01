package org.easybi.first;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQueryException;

import static org.apache.spark.sql.functions.count;

import java.util.concurrent.TimeoutException;

public class SocketExample {

    public static void main(String args[]) throws StreamingQueryException, TimeoutException {

       SparkSession
                .builder()
                .master("local")
                .getOrCreate()
                .readStream()
                .format("socket")
                .option("host", "localhost")
                .option("port", 50505)
                .load()
                .groupBy("value").agg(count("value").as("count"))
                .writeStream()
                .format("console")
                .outputMode(OutputMode.Complete())
                //.trigger(ProcessingTime("30 seconds"))
                .start()
                .awaitTermination();
    }
}
