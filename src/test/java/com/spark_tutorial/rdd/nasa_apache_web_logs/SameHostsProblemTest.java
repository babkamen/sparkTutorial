package com.spark_tutorial.rdd.nasa_apache_web_logs;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReaderBuilder;
import com.spark_tutorial.SparkUtils;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SameHostsProblemTest {


    @Test
    void process() throws Exception {
        final String[] inputFiles = {"in/nasa_19950701.tsv", "in/nasa_19950801.tsv"};
        List<String> expectedResult = null;

        for (String inputFile : inputFiles) {
            assert new File(inputFile).exists();
            final CSVParser parser = new CSVParserBuilder()
                    .withSeparator('\t').build();
            var reader = new CSVReaderBuilder(new FileReader(inputFile)).withCSVParser(parser)
                    .withSkipLines(1).build();
            List<String> hosts = reader.readAll().parallelStream().map(e -> e[0]).collect(Collectors.toList());
            if (expectedResult == null) {
                expectedResult = hosts;
            } else {
                expectedResult.retainAll(hosts);
            }
        }
        expectedResult = new ArrayList<>(new HashSet<>(expectedResult));

        final SparkSession ss = SparkUtils.getOrCreateSession();
        var outputFilePath = "out/nasa_logs_same_hosts.csv";
        Path currentRelativePath = Paths.get("");
        String currentPath = currentRelativePath.toAbsolutePath().toString();
        final String partFilePath = currentPath + "/" + outputFilePath + "/part-00000";

        SameHostsProblem.process(ss, outputFilePath, inputFiles);

        final List<String> result = new Scanner(new FileInputStream(partFilePath), StandardCharsets.UTF_8)
                .useDelimiter("\\n")
                .tokens().collect(Collectors.toList());
        expectedResult.sort(Comparator.naturalOrder());
        result.sort(Comparator.naturalOrder());
        assertEquals(expectedResult, result);
    }
}