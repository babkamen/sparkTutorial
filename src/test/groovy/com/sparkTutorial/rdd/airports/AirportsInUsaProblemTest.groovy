package com.sparkTutorial.rdd.airports

import spock.lang.Specification


class AirportsInUsaProblemTest extends Specification {
    def "happy path"() {

        when:
        def testClass = new AirportsInUsaProblem()
        testClass.main(new String[]{})

        then:
        def airports = [:]
        def file = new File(AirportsInUsaProblem.OUTPUT_FILE)
        println file.absolutePath
        def expectedCount = 1697
        def count = 0

        def l=file.eachLine { line,i ->
            if(i==1)return
            def fields=line.split(",")
            airports.put(fields[0], fields[1])
            count++
        }
        expectedCount == count

    }
}
