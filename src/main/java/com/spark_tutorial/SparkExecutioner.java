package com.spark_tutorial;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.function.Consumer;

public class SparkExecutioner {

    private SparkExecutioner() {
    }

    public static void doInJavaSparkContext(SparkConf conf, Consumer<JavaSparkContext> consumer) {
        try (final JavaSparkContext jsc = new JavaSparkContext(conf)) {
            consumer.accept(jsc);
        }
    }
}
