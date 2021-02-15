package com.spark_tutorial.rdd.sum_of_numbers;

import com.spark_tutorial.SparkUtils;
import org.junit.jupiter.api.Test;

import java.io.FileReader;
import java.util.Scanner;
import java.util.function.Predicate;

import static com.spark_tutorial.rdd.sum_of_numbers.SumOfNumbersProblem.DELIMETER;
import static org.junit.jupiter.api.Assertions.assertEquals;

class SumOfNumbersProblemTest {

    @Test
    void process() throws Exception {
        int expectedSum = 0;
        String input = "in/prime_nums.text";
        var reader = new Scanner(new FileReader(input)).useDelimiter(DELIMETER);
        var expectedResult = reader.tokens().filter(Predicate.not(String::isBlank)).map(Integer::parseInt).reduce(Integer::sum)
                .orElseThrow(()->new RuntimeException("Cannot find integers in file"));

        final Number result = SumOfNumbersProblem.process(SparkUtils.getOrCreateJavaSparkContext(), input);

        assertEquals(expectedResult, result);
    }
}