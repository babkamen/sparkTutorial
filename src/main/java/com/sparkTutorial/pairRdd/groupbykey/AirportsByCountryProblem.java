package com.sparkTutorial.pairRdd.groupbykey;

import com.sparkTutorial.SparkUtils;
import com.sparkTutorial.rdd.airports.AbstractAirportsProblem;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import static com.sparkTutorial.rdd.airports.WordSparkUtils.*;

public class AirportsByCountryProblem extends AbstractAirportsProblem {

    /* Create a Spark program to read the airport data from in/airports.text,
       output the the list of the names of the airports located in each country.

       Each row of the input file contains the following columns:
       Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
       ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

       Sample output:

       "Canada", ["Bagotville", "Montreal", "Coronation", ...]
       "Norway" : ["Vigra", "Andenes", "Alta", "Bomoen", "Bronnoy",..]
       "Papua New Guinea",  ["Goroka", "Madang", ...]
       ...
     */
    public JavaPairRDD<String, Iterable<String>> processData(String inputFilepath) {
        final SparkSession sc = SparkUtils.setup();
        final Dataset<Row> rdd = readAirportsFile(sc, inputFilepath);
        //output the the list of the names of the airports located in each country.
        return rdd.toJavaRDD()
                .mapToPair(row -> new Tuple2<String, String>(row.getAs(COUNTRY), row.getAs(NAME)))
                .groupByKey();
    }

    @Override
    protected void saveToFile(Object selectAndFilter, String partsFolderPath) {

    }

    @Override
    protected Object selectAndFilter(Dataset rdd) {
        return null;
    }
}
