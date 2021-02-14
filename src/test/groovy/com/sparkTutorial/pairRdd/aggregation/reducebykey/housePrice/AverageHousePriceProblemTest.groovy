package com.sparkTutorial.pairRdd.aggregation.reducebykey.housePrice

import com.opencsv.CSVReader
import org.apache.commons.math3.util.Pair
import spock.lang.Specification

import java.util.stream.Collectors

class AverageHousePriceProblemTest extends Specification {
    def "Process"() {
        when:
        def res = AverageHousePriceProblem.process().entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().doubleValue()))
        then:
        def reader = new CSVReader(new FileReader(new File(AverageHousePriceProblem.INPUT_FILE)))
        reader.skip(1)
        List<String[]> rows = reader.readAll()
        //MLS,Location,Price,Bedrooms,Bathrooms,Size,Price SQ Ft,Status
        def expectedResult = rows.stream()
                .map({ new Pair<>(Integer.parseInt(it[3]), new BigDecimal(it[2])) })
                .collect(Collectors.groupingBy(Pair::getKey,new AverageProductPriceCollector()));
        res == expectedResult
        println expectedResult
//        println res
    }
}
