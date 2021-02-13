package com.sparkTutorial.pairRdd.filter;

import com.sparkTutorial.rdd.airports.AbstractAirportsProblem;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

@Slf4j
public class AirportsNotInUsaProblem extends AbstractAirportsProblem<JavaPairRDD<Object, Object>> {

    public static final String PARTS_FOLDER_PATH = "out/airports_not_in_usa";
    public static final String OUTPUT_FILE = "out/airports_not_in_usa_pair_rdd.text";

    /* Create a Spark program to read the airport data from in/airports.text;
       generate a pair RDD with airport name being the key and country name being the value.
       Then remove all the airports which are located in United States and output the pair RDD to out/airports_not_in_usa_pair_rdd.text

       Each row of the input file contains the following columns:
       Airport ID, Name of airport, Main city served by airport, Country where airport is located,
       IATA/FAA code, ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

       Sample output:

       ("Kamloops", "Canada")
       ("Wewak Intl", "Papua New Guinea")
       ...
     */
    public static void main(String[] args) throws Exception {
        new AirportsNotInUsaProblem().processData(PARTS_FOLDER_PATH, OUTPUT_FILE);
    }

    protected void renamePartsToFilename(String partsFolder, String filepath) throws IOException {
        final File dir = new File(partsFolder);
        log.info(dir.getAbsolutePath());
        final File[] files = new File(partsFolder).listFiles((file, s) -> s.startsWith("part-")&&!s.contains("."));
        assert files != null && files.length > 0;
        log.info(Arrays.toString(files));
        FileUtils.moveFile(files[0], new File(filepath));
    }

    @Override
    protected void saveToFile(JavaPairRDD<Object, Object> rdd, String outputFile) {
        rdd.saveAsTextFile(outputFile);
    }

    @Override
    protected JavaPairRDD<Object, Object> selectAndFilter(Dataset<Row> rdd) {
        return rdd.select(NAME, CITY).filter(COUNTRY + "<>'United States'")
                .toJavaRDD().mapToPair(row -> new Tuple2<>(row.get(0), row.get(1)));
    }
}
