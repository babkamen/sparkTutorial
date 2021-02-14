package com.spark_tutorial.rdd.take;

import com.spark_tutorial.SparkExecutioner;
import lombok.extern.slf4j.Slf4j;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;

import java.util.Arrays;
import java.util.List;

@Slf4j
public class TakeExample {

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.OFF);
        SparkConf conf = new SparkConf().setAppName("take").setMaster("local[*]");
        SparkExecutioner.doInJavaSparkContext(conf, sc -> {

            List<String> inputWords = Arrays.asList("spark", "hadoop", "spark", "hive", "pig", "cassandra", "hadoop");
            JavaRDD<String> wordRdd = sc.parallelize(inputWords);

            List<String> words = wordRdd.take(3);

            for (String word : words) {
                System.out.println(word);
            }
        });
    }
}
