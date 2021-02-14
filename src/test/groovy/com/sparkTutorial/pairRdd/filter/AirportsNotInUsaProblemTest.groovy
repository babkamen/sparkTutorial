package com.sparkTutorial.pairRdd.filter


import spock.lang.Specification

class AirportsNotInUsaProblemTest extends Specification {
    private String partsFolder = "out/airports_not_in_usa";
    private String outputFile = "out/airports_not_in_usa_pair_rdd.text";
    private String inputFile = "in/airports.text";

    def "happy path"() {

        when:
        new AirportsNotInUsaProblem().processData(inputFile, partsFolder, outputFile)

        then:
        def file = new File(outputFile)
        file.exists()
        println file.absolutePath
        def expectedCount = 6410
        def count = 0

        def l = file.eachLine { line, i ->
            count++
        }
        expectedCount == count

    }
}
