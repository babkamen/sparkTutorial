package com.sparkTutorial.rdd.airports;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;


public class AirportsInUsaProblem extends AbstractAirportsProblemCSV {

    public static final String PARTS_FOLDER = "out/airport_in_usa";
    public static final String OUTPUT_FILE = "out/airports_in_usa.text";

    /* Create a Spark program to read the airport data from in/airports.text, find all the airports which are located in United States
               and output the airport's name and the city's name to out/airports_in_usa.text.

               Each row of the input file contains the following columns:
               Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
               ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

               Sample output:
               "Putnam County Airport", "Greencastle"
               "Dowagiac Municipal Airport", "Dowagiac"
               ...
             */
    public static void main(String[] args) throws Exception {
        new AirportsInUsaProblem().processData(PARTS_FOLDER, OUTPUT_FILE);
    }

    public Dataset<Row> selectAndFilter(Dataset<Row> rdd) {
        return rdd.select(NAME, CITY).filter(COUNTRY+"='United States'");
    }
}
