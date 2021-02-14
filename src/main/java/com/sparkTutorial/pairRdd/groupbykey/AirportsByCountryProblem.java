package com.sparkTutorial.pairRdd.groupbykey;

import com.sparkTutorial.SparkUtils;
import com.sparkTutorial.rdd.airports.AbstractAirportsProblem;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Map;

public class AirportsByCountryProblem extends AbstractAirportsProblem {


    public static void main(String[] args) throws Exception {
        new AirportsByCountryProblem().processData();
    }


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
    public Map<Object, Iterable<Object>> processData() {
        final SparkSession sc = SparkUtils.setup();
        final Dataset<Row> rdd = readAirportsFile(sc);
        //output the the list of the names of the airports located in each country.
        final JavaPairRDD<Object, Object> objectObjectJavaPairRDD = rdd.toJavaRDD().mapToPair(row -> new Tuple2<>(row.getAs(COUNTRY), row.getAs(NAME)));
        System.out.println(objectObjectJavaPairRDD.collect());
        return objectObjectJavaPairRDD.groupByKey().collectAsMap();
    }

    @Override
    protected void saveToFile(Object selectAndFilter, String partsFolderPath) {

    }

    @Override
    protected Object selectAndFilter(Dataset rdd) {
        return null;
    }
}
