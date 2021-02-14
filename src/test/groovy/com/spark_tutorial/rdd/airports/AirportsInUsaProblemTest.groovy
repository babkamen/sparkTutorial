package com.spark_tutorial.rdd.airports

import spock.lang.Specification


class AirportsInUsaProblemTest extends Specification {

    private String partsFolder = "out/airport_in_usa";
    private String outputFile = "out/airports_in_usa.text";
    private String inputFile = "in/airports.text";

    def "test get airports names where country=US"() {

        when:
        new AirportsInUsaProblem().processData(inputFile, partsFolder, outputFile)

        then:
        def file = new File(outputFile)
        file.exists()
        println file.absolutePath
        def expectedCount = 1697
        def count = 0

        def l = file.eachLine { line, i ->
            if (i > 1) count++
            if (i == 2) {
                assert line == "\"Putnam, County Airport\",Greencastle"
            }
        }
        expectedCount == count

    }
}
