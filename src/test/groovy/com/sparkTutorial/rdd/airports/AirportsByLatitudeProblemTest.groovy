package com.sparkTutorial.rdd.airports

import spock.lang.Specification

class AirportsByLatitudeProblemTest extends Specification {
    def "filter records where latitude>40"() {

        when:
        new AirportsByLatitudeProblem().main(new String[]{})

        then:
        def file = new File(AirportsByLatitudeProblem.OUTPUT_FILE)
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
