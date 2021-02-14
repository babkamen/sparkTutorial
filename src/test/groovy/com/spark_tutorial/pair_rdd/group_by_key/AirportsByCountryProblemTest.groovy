package com.spark_tutorial.pair_rdd.group_by_key


import com.opencsv.CSVReader
import org.apache.commons.math3.util.Pair
import spock.lang.Specification

import java.util.stream.Collectors

class AirportsByCountryProblemTest extends Specification {

    //        Each row of the input file contains the following columns:
    //        Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
    //        ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format
    def "happy path"() {
        given:
        def inputFile = "in/airports.text"
        def reader = new CSVReader(new FileReader(new File(inputFile)))
        List<String[]> rows = reader.readAll()
        def expectedResult = rows.parallelStream().map({ new Pair<>(it[3], it[1]) })
                .collect(Collectors.groupingBy(Pair::getKey, Collectors.mapping(Pair::getValue, Collectors.toList())));

        when:
        def result = new AirportsByCountryProblem().processData(inputFile).collect().parallelStream()
                .collect(Collectors.toMap(e -> e._1, e -> new ArrayList<>(e._2)))
        then:
        result == expectedResult

    }
}
