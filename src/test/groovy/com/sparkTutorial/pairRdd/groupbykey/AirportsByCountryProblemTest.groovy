package com.sparkTutorial.pairRdd.groupbykey

import com.opencsv.CSVReader
import org.apache.commons.math3.util.Pair
import spock.lang.Specification

import java.util.stream.Collectors

class AirportsByCountryProblemTest extends Specification {
    def "happy path"() {
        given:
        def reader = new CSVReader(new FileReader(new File(AirportsByCountryProblem.INPUT_FILE)))
        reader.skip(1)
        List<String[]> rows = reader.readAll()

        def expectedResult = rows.parallelStream().map({ new Pair<>(it[3], it[2]) })
                .collect(Collectors.groupingBy(Pair::getKey, Collectors.mapping(Pair::getValue, Collectors.toList())));

        when:
        def result = new AirportsByCountryProblem().processData()
        then:
        expectedResult == result
    }
}
