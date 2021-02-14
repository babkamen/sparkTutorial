package com.spark_tutorial.rdd.count;

import com.spark_tutorial.SparkExecutioner;
import lombok.extern.slf4j.Slf4j;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

@Slf4j
public class CountExample {

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setAppName("count").setMaster("local[*]");
        SparkExecutioner.doInJavaSparkContext(conf, sc -> {
            List<String> inputWords = Arrays.asList("spark", "hadoop", "spark", "hive", "pig", "cassandra", "hadoop");
            JavaRDD<String> wordRdd = sc.parallelize(inputWords);

            System.out.println("Count: " + wordRdd.count());

            Map<String, Long> wordCountByValue = wordRdd.countByValue();

            System.out.println("CountByValue:");

            wordCountByValue.forEach((k, v) -> System.out.println(k + ":" + v));
        });
    }
}
