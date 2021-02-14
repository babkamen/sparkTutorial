package com.sparkTutorial.pairRdd.sort;


import com.sparkTutorial.SparkUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;

public class SortedWordCountProblem {

    public static final String SPACE_DELIMETER = "\\s";

    /* Create a Spark program to read the an article from in/word_count.text,
               output the number of occurrence of each word in descending order.

               Sample output:

               apple : 200
               shoes : 193
               bag : 176
               ...
             */
    public static JavaPairRDD<String, Integer> process(String inputFile){
        final SparkSession sc = SparkUtils.setup();
        Logger.getLogger("org").setLevel(Level.ERROR);
        final JavaSparkContext jsc = new JavaSparkContext(sc.sparkContext());
        JavaRDD<String> lines = jsc.textFile(inputFile);
        JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(SPACE_DELIMETER)).iterator());
        System.out.println(words.collect());
        return words.mapToPair(s -> new Tuple2<>(s, 1)).reduceByKey(Integer::sum);
                //.sortByKey(false);
    }
}

