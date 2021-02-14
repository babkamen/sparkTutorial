package com.sparkTutorial.pairRdd.sort;

import org.junit.jupiter.api.Assertions;

import java.io.FileReader;
import java.util.Map;
import java.util.Scanner;
import java.util.TreeMap;
import java.util.function.Function;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.summingInt;

class SortedWordCountProblemTest {

    @org.junit.jupiter.api.Test
    void process() throws Exception {
        final String fileName = "in/word_count.text";
        Map<String, Integer> expectedResult;
        try (var reader = new Scanner(new FileReader(fileName)).useDelimiter(SortedWordCountProblem.SPACE_DELIMETER)) {
            expectedResult = reader.tokens().collect(groupingBy(Function.identity(), summingInt(e -> 1)));
        }

        final Map<String, Integer> result = SortedWordCountProblem.process(fileName).collectAsMap();

        Assertions.assertEquals(new TreeMap<>(result), new TreeMap<>(expectedResult));
    }
}