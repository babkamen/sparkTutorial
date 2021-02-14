package com.spark_tutorial.spark_sql;

import com.opencsv.bean.CsvToBeanBuilder;
import com.spark_tutorial.SparkUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.FileReader;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import static com.spark_tutorial.spark_sql.HousePriceProblem.AVG_PRICE_COLUMN_NAME;
import static com.spark_tutorial.spark_sql.HousePriceProblem.MAX_PRICE_COLUMN_NAME;
import static java.util.stream.Collectors.groupingBy;
import static org.junit.jupiter.api.Assertions.assertEquals;

class HousePriceProblemTest {

    public static final RoundingMode ROUNDING_MODE = RoundingMode.HALF_EVEN;

    @DisplayName("group by location, aggregate the average price per SQ Ft and max price, and sort by average price per SQ Ft")
    @Test
    void process() throws Exception {
        final String inputFile = "in/RealEstate_small.csv";
        var reader = new CsvToBeanBuilder<RetailRecord>(new FileReader(inputFile))
                .withType(RetailRecord.class)
                .build();

        final Map<String, RetailStats> expectedResult = calculateStats(reader);

        final Dataset<Row> rdd = HousePriceProblem.process(SparkUtils.getOrCreateSession(), inputFile);

        final List<RetailStats> result = rdd.collectAsList().parallelStream().map(v -> {
            final RetailStats rs = new RetailStats();
            rs.setLocation(v.getAs("Location"));
            rs.setMaxPrice(new BigDecimal(v.getAs(MAX_PRICE_COLUMN_NAME).toString()));
            rs.setAvgPricePerSqFt(new BigDecimal(v.getAs(AVG_PRICE_COLUMN_NAME).toString()).setScale(0, RoundingMode.HALF_UP));
            return rs;
        }).collect(Collectors.toList());

        //sort actual and result lists
        final Comparator<RetailStats> comparing = Comparator.comparing(RetailStats::getLocation);
        result.sort(comparing);
        final List<RetailStats> expectedResultList = expectedResult.values().stream()
                .sorted(Comparator.comparing(RetailStats::getLocation))
                .collect(Collectors.toList());

        assertEquals(expectedResultList, result);
    }

    @NotNull
    private Map<String, RetailStats> calculateStats(com.opencsv.bean.CsvToBean<RetailRecord> reader) {
        final Map<String, RetailStats> expectedResult = reader.stream().parallel().collect(groupingBy(RetailRecord::getLocation, Collector.of(
                RetailStats::new,
                (a, p) -> {
                    a.setMaxPrice(p.getPrice().max(a.getMaxPrice()));
                    a.incCount();
                    a.setLocation(p.getLocation());
                    a.setAvgPricePerSqFtSum(a.getAvgPricePerSqFtSum().add(p.getPricePerSqFt()));
                }, (a, b) -> {
                    final RetailStats t = new RetailStats();
                    t.setCount(a.getCount() + b.getCount());
                    t.setAvgPricePerSqFtSum(a.getAvgPricePerSqFtSum().add(b.getAvgPricePerSqFtSum()));
                    t.setMaxPrice(a.getMaxPrice().max(b.getMaxPrice()));
                    t.setLocation(a.getLocation());
                    return t;
                }
        )));
        expectedResult.forEach((k, v) -> {
            final BigDecimal avg = v.getAvgPricePerSqFtSum().divide(new BigDecimal(String.valueOf(v.getCount())), ROUNDING_MODE);
            v.setAvgPricePerSqFt(avg.setScale(0, RoundingMode.HALF_UP));
            v.setAvgPricePerSqFtSum(BigDecimal.ZERO);
            v.setCount(0);
        });
        return expectedResult;
    }
}