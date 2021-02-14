package com.spark_tutorial.rdd;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Uppercase {

    public static void main(String[] args) {
        // Create a Java Spark Context.
        SparkConf conf = new SparkConf().setAppName("uppercase").setMaster("local[*]");

        try (JavaSparkContext sc = new JavaSparkContext(conf)) {

            JavaRDD<String> lines = sc.textFile("in/uppercase.text");
            JavaRDD<String> lowerCaseLines = lines.map(String::toUpperCase);

            lowerCaseLines.saveAsTextFile("out/uppercase.text");
        }
    }
}
