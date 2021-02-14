package com.spark_tutorial.advanced.accumulator;

import com.spark_tutorial.SparkUtils;
import com.spark_tutorial.rdd.commons.Utils;
import lombok.extern.slf4j.Slf4j;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.LongAccumulator;
import scala.Option;

import java.nio.charset.StandardCharsets;

@Slf4j
public class StackOverFlowSurvey {

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);

        final SparkConf conf = SparkUtils.createConf();
        JavaSparkContext jsc = new JavaSparkContext(conf);
        final SparkContext sc = jsc.sc();

        final LongAccumulator total = new LongAccumulator();
        final LongAccumulator missingSalaryMidPoint = new LongAccumulator();
        final LongAccumulator bytesProcessed = new LongAccumulator();

        total.register(sc, Option.apply("total"), false);
        missingSalaryMidPoint.register(sc, Option.apply("missing salary middle point"), false);
        bytesProcessed.register(sc, Option.apply("bytes processed"), false);

        JavaRDD<String> responseRDD = jsc.textFile("in/2016-stack-overflow-survey-responses.csv");

        JavaRDD<String> responseFromCanada = responseRDD.filter(response -> {
            String[] splits = response.split(Utils.COMMA_DELIMITER, -1);
            bytesProcessed.add(response.getBytes(StandardCharsets.UTF_8).length);
            total.add(1);

            if (splits[14].isEmpty()) {
                missingSalaryMidPoint.add(1);
            }

            return splits[2].equals("Canada");

        });

        System.out.println("Count of responses from Canada: " + responseFromCanada.count());
        System.out.println("Total count of responses: " + total.value());
        System.out.println("Count of responses missing salary middle point: " + missingSalaryMidPoint.value());
        System.out.println("Bytes processed: " + bytesProcessed.value());
    }
}
