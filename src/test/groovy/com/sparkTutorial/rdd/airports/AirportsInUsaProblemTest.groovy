package com.sparkTutorial.rdd.airports

import spock.lang.Specification


class AirportsInUsaProblemTest extends Specification {
    def "happy path"() {

        when:
        new AirportsInUsaProblem().main(new String[]{})

        then:
        def file = new File(AirportsInUsaProblem.OUTPUT_FILE)
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
