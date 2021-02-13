package com.sparkTutorial.pairRdd.filter

import com.sparkTutorial.rdd.airports.AirportsInUsaProblem
import spock.lang.Specification

class AirportsNotInUsaProblemTest extends Specification {
    def "happy path"() {

        when:
        new AirportsNotInUsaProblem().main(new String[]{})

        then:
        def file = new File(AirportsNotInUsaProblem.OUTPUT_FILE)
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
