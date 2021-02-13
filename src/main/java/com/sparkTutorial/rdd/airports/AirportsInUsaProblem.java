package com.sparkTutorial.rdd.airports;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;

import static org.apache.spark.sql.types.DataTypes.*;

@Slf4j
public class AirportsInUsaProblem {

    public static final String AIRPORT_ID = "Airport ID";
    public static final String NAME = "Name";
    public static final String CITY = "City";
    public static final String COUNTRY = "Country";
    public static final String INPUT_FILE = "in/airports.text";
    public static final String OUTPUT_FILE = "out/airports_in_usa.text";
    public static final String OUT_PARTS_FOLDER = "out/airport/";

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
//        Logger.getLogger("org").setLevel(Level.ERROR);
        final SparkSession sc = SparkSession.builder()
                .master("local[*]")
                .appName(AirportsInUsaProblem.class.getName())
                .getOrCreate();
        final Dataset<Row> rdd = readFile(sc);

        deleteDir(OUT_PARTS_FOLDER);
        deleteFile(OUTPUT_FILE);
        rdd.select(NAME, CITY).filter("Country='United States'")
                .coalesce(1)
                .write()
                .format("com.databricks.spark.csv")
                .option("header", "true")
                .save(OUT_PARTS_FOLDER);
        renamePartsToFilename(OUT_PARTS_FOLDER, OUTPUT_FILE);
        deleteDir(OUT_PARTS_FOLDER);
    }

    private static Dataset<Row> readFile(SparkSession sc) {
        StructType structType = new StructType(new StructField[]{
                new StructField(AIRPORT_ID, IntegerType, true, Metadata.empty()),
                new StructField(NAME, StringType, true, Metadata.empty()),
                new StructField(CITY, StringType, true, Metadata.empty()),
                new StructField(COUNTRY, StringType, true, Metadata.empty()),
                new StructField("IATA/FAA code", StringType, true, Metadata.empty()),
                new StructField("ICAO Code", StringType, true, Metadata.empty()),

                new StructField("Latitude", DoubleType, true, Metadata.empty()),
                new StructField("Longitude", DoubleType, true, Metadata.empty()),
                new StructField("Altitude", DoubleType, true, Metadata.empty()),
                new StructField("Timezone", IntegerType, true, Metadata.empty()),
                new StructField("DST", StringType, true, Metadata.empty()),
                new StructField("Timezone in Olson format", StringType, true, Metadata.empty())
        });

        return sc.read()
                .format("com.databricks.spark.csv")
                .option("header", "false") // Use first line of all files as header
                .schema(structType)
                .load(INPUT_FILE);
    }

    private static void renamePartsToFilename(String partsFolder, String filepath) throws IOException {
        final File dir = new File(partsFolder);
        log.info(dir.getAbsolutePath());
        final File[] files = new File(OUT_PARTS_FOLDER).listFiles((file, s) -> s.endsWith(".csv"));
        assert files != null && files.length > 0;
        log.info(Arrays.toString(files));
        FileUtils.moveFile(files[0], new File(filepath));
    }

    private static void deleteDir(String path) throws IOException {
        FileUtils.deleteDirectory(new File(path));
    }

    public static void deleteFile(String path) throws IOException {
        Files.delete(new File(path).toPath());
    }
}
