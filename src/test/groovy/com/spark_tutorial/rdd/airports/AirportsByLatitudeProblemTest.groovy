package com.spark_tutorial.rdd.airports

import spock.lang.Specification

class AirportsByLatitudeProblemTest extends Specification {

    private String partsFolder = "out/airports_by_latitude";
    private String outputFile = "out/airports_by_latitude.text";
    private String inputFile = "in/airports.text";

    def "filter records where latitude>40"() {

        when:
        new AirportsByLatitudeProblem().processData(inputFile, partsFolder, outputFile)

        then:
        def file = new File(outputFile)
        file.exists()
        println file.absolutePath

        def l = file.eachLine { line, i ->
            if (i > 1) {
                def s = line.substring(line.lastIndexOf(",") + 1)
                assert Double.parseDouble(s) > 40: " Latitude is less than 40 on line " + i
            }
        }
    }
}
