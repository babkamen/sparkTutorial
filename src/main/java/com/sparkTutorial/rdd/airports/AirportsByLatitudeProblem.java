package com.sparkTutorial.rdd.airports;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.File;

public class AirportsByLatitudeProblem extends AbstractAirportsProblem {


    public static final String PARTS_FOLDER_PATH = "out/airports_by_latitude";
    public static final String OUTPUT_FILE = "out/airports_by_latitude.text";

    /*
               Create a Spark program to read the airport data from in/airports.text,  find all the airports whose latitude are bigger than 40.
               Then output the airport's name and the airport's latitude to out/airports_by_latitude.text.

               Each row of the input file contains the following columns:
               Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
               ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

               Sample output:
               "St Anthony", 51.391944
               "Tofino", 49.082222
               ...
             */
    public static void main(String[] args) throws Exception {
        new AirportsByLatitudeProblem().processData(new File(PARTS_FOLDER_PATH).getAbsolutePath(), OUTPUT_FILE);
    }

    @Override
    protected Dataset<Row> selectAndFilter(Dataset<Row> rdd) {
        return rdd.select(NAME, LATITUDE).filter(LATITUDE + ">40");
    }
}
