package com.spark_tutorial.pair_rdd.sort;


import lombok.extern.slf4j.Slf4j;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

@Slf4j
public class SortedWordCountProblem {

    public static final String SPACE_DELIMETER = "\\s";

    private SortedWordCountProblem() {
    }

    /**
     * Create a Spark program to read the an article from in/word_count.text,
     * output the number of occurrence of each word in descending order.
     * <p>
     * Sample output:
     * <p>
     * apple : 200
     * shoes : 193
     * bag : 176
     * ...
     */
    public static JavaPairRDD<String, Integer> process(JavaSparkContext jsc, String inputFile) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        JavaRDD<String> lines = jsc.textFile(inputFile);
        JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(SPACE_DELIMETER)).iterator());
        System.out.println(words.collect());
        return words.mapToPair(s -> new Tuple2<>(s, 1)).reduceByKey(Integer::sum)
                .mapToPair(Tuple2::swap)
                .sortByKey(false)
                .mapToPair(Tuple2::swap);
    }
}

