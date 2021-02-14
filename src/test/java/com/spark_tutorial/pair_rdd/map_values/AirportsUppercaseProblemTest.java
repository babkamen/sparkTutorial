package com.spark_tutorial.pair_rdd.map_values;

import com.opencsv.CSVReader;
import com.spark_tutorial.SparkUtils;
import org.apache.commons.math3.util.Pair;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.FileReader;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import static com.spark_tutorial.rdd.airports.WordSparkUtils.COUNTRY;
import static com.spark_tutorial.rdd.airports.WordSparkUtils.NAME;
import static org.junit.Assert.assertEquals;

class AirportsUppercaseProblemTest {

    @Test
    void testCountryNameToUppercase() throws Exception {
        final SparkSession ss = SparkUtils.getOrCreateSession();
        var inputFile = "in/airports.text";
        var reader = new CSVReader(new FileReader(inputFile));
        List<String[]> rows = reader.readAll();
        var expectedResult = rows.parallelStream()
                .map(e -> new Pair<>(e[1].toUpperCase(), e[3])).sorted(Comparator.comparing(Pair::getValue)).collect(Collectors.toList());

        final Dataset<Row> dataset = AirportsUppercaseProblem.process(ss, inputFile);

        var result = dataset.collectAsList().parallelStream()
                .map(row -> new Pair<>(row.getAs(NAME), row.getAs(COUNTRY))).collect(Collectors.toList());
        Assertions.assertEquals(result, expectedResult);
    }
}