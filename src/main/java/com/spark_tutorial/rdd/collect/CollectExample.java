package com.spark_tutorial.rdd.collect;

import lombok.extern.slf4j.Slf4j;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

@Slf4j
public class CollectExample {

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setAppName("collect").setMaster("local[*]");
        try (JavaSparkContext sc = new JavaSparkContext(conf)) {

            List<String> inputWords = Arrays.asList("spark", "hadoop", "spark", "hive", "pig", "cassandra", "hadoop");
            JavaRDD<String> wordRdd = sc.parallelize(inputWords);

            List<String> words = wordRdd.collect();

            for (String word : words) {
                System.out.println(word);
            }
        }
    }
}
