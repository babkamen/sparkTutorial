package com.spark_tutorial.rdd.sum_of_numbers;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

public class SumOfNumbersProblem {


    public static final String DELIMETER = "\\s+";

    /**
     * Create a Spark program to read the first 100 prime numbers from in/prime_nums.text,
     * print the sum of those numbers to console.
     * <p>
     * Each row of the input file contains 10 prime numbers separated by spaces.
     */
    public static Number process(JavaSparkContext jsc, String inputFile) throws Exception {
        JavaRDD<String> lines = jsc.textFile(inputFile);
        return lines.flatMap(line -> Arrays.asList(line.split(DELIMETER)).iterator())
                .filter(v1 -> !v1.isBlank())
                .map(Integer::parseInt).reduce(Integer::sum);
    }
}
