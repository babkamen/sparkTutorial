package com.spark_tutorial.pair_rdd.sort;

import com.spark_tutorial.SparkUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.FileReader;
import java.util.Collections;
import java.util.Map;
import java.util.Scanner;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.summingInt;
import static org.hamcrest.MatcherAssert.assertThat;

class SortedWordCountProblemTest {

    @Test
    void process() throws Exception {
        final String fileName = "in/word_count.text";
        var reader = new Scanner(new FileReader(fileName)).useDelimiter(SortedWordCountProblem.SPACE_DELIMETER);
        var mapping = Collectors.mapping(Map.Entry<String, Integer>::getKey, Collectors.toList());
        var groupingBy = groupingBy(Map.Entry::getValue,
                TreeMap::new,
                mapping);
        // count words frequency, and save frequencies as key and words as values in map
        var expectedResult = reader.tokens().collect(groupingBy(Function.identity(), summingInt(e -> 1))).entrySet()
                .stream()
                .collect(groupingBy);


        final JavaPairRDD<String, Integer> rdd = SortedWordCountProblem.process(SparkUtils.getOrCreateJavaSparkContext(), fileName);

        final var map = rdd.collectAsMap();
        //check if descending order
        int t = Integer.MAX_VALUE;
        for (var e : rdd.collect()) {
            assertThat(e._2, Matchers.lessThanOrEqualTo(t));
            t = e._2();
        }
        var result = map.entrySet().stream().collect(groupingBy);

        expectedResult.values().forEach(Collections::sort);
        result.values().forEach(Collections::sort);

        Assertions.assertEquals(expectedResult, result);
        reader.close();
    }
}