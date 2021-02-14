package com.sparkTutorial.pairRdd.aggregation.reducebykey.housePrice

import com.opencsv.CSVReader
import org.apache.commons.math3.util.Pair
import scala.Tuple2
import spock.lang.Specification

import java.util.stream.Collectors

class AverageHousePriceProblemTest extends Specification {
    def "happy path"() {
        given:
        def reader = new CSVReader(new FileReader(new File(AverageHousePriceProblem.INPUT_FILE)))
        reader.skip(1)
        List<String[]> rows = reader.readAll()

        def expectedResult = rows.parallelStream()
                .map({ new Pair<Integer, BigDecimal>(Integer.parseInt(it[3]), new BigDecimal(it[2])) })
                .collect(Collectors.groupingBy(Pair::getKey, new AverageProductPriceCollector(AverageHousePriceProblem.ROUNDING_MODE)))
                .entrySet()
                .stream()
                .map({ return new Tuple2<>(it.getKey(), it.getValue()) })
                .sorted(Comparator.comparing({ v -> (Integer) v._1 }))
                .collect(Collectors.toList());
        when:
        def res = AverageHousePriceProblem.process().collect()
        then:
        res == expectedResult
        and: 'result is sorted'

    }
}
