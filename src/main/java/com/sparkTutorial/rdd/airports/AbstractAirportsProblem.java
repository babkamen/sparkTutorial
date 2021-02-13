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
public abstract class AbstractAirportsProblem {
    public static final String AIRPORT_ID = "Airport ID";
    public static final String NAME = "Name";
    public static final String CITY = "City";
    public static final String COUNTRY = "Country";
    public static final String INPUT_FILE = "in/airports.text";
    public static final String FAA_CODE = "IATA/FAA code";
    public static final String ICAO_CODE = "ICAO Code";
    public static final String LATITUDE = "Latitude";
    public static final String LONGITUDE = "Longitude";
    public static final String ALTITUDE = "Altitude";
    public static final String TIMEZONE = "Timezone";
    public static final String DST = "DST";
    public static final String TIMEZONE_IN_OLSON_FORMAT = "Timezone in Olson format";

    /**
     * Read airports file select/filter and save it to output file
     * @param partsFolderPath - folder that spark uses to stores output
     * @throws IOException
     */
    protected  void processData(String partsFolderPath, String outputFile) throws IOException {
        final SparkSession sc = setup();

        deleteDir(partsFolderPath);
        deleteFile(outputFile);

        final Dataset<Row> rdd = readAirportsFile(sc);
        selectAndFilter(rdd)
                .coalesce(1)
                .write()
                .format("com.databricks.spark.csv")
                .option("header", "true")
                .save(partsFolderPath);
        renamePartsToFilename(partsFolderPath, outputFile);

//        deleteDir(partsFolderPath);
    }

    protected abstract Dataset<Row> selectAndFilter(Dataset<Row> rdd);

    protected static SparkSession setup() {
        Logger.getLogger("org").setLevel(Level.ERROR);
        return SparkSession.builder()
                .master("local[*]")
                .appName(AirportsInUsaProblem.class.getName())
                .getOrCreate();
    }

    protected  Dataset<Row> readAirportsFile(SparkSession sc) {
        StructType structType = new StructType(new StructField[]{
                new StructField(AIRPORT_ID, IntegerType, true, Metadata.empty()),
                new StructField(NAME, StringType, true, Metadata.empty()),
                new StructField(CITY, StringType, true, Metadata.empty()),
                new StructField(COUNTRY, StringType, true, Metadata.empty()),
                new StructField(FAA_CODE, StringType, true, Metadata.empty()),
                new StructField(ICAO_CODE, StringType, true, Metadata.empty()),

                new StructField(LATITUDE, DoubleType, true, Metadata.empty()),
                new StructField(LONGITUDE, DoubleType, true, Metadata.empty()),
                new StructField(ALTITUDE, DoubleType, true, Metadata.empty()),
                new StructField(TIMEZONE, IntegerType, true, Metadata.empty()),
                new StructField(DST, StringType, true, Metadata.empty()),
                new StructField(TIMEZONE_IN_OLSON_FORMAT, StringType, true, Metadata.empty())
        });

        return sc.read()
                .format("com.databricks.spark.csv")
                .option("header", "false") // Use first line of all files as header
                .schema(structType)
                .load(INPUT_FILE);
    }

    protected  void renamePartsToFilename(String partsFolder, String filepath) throws IOException {
        final File dir = new File(partsFolder);
        log.info(dir.getAbsolutePath());
        final File[] files = new File(partsFolder).listFiles((file, s) -> s.endsWith(".csv"));
        assert files != null && files.length > 0;
        log.info(Arrays.toString(files));
        FileUtils.moveFile(files[0], new File(filepath));
    }

    protected void deleteDir(String path) throws IOException {
        final File directory = new File(path);
        if (directory.exists()) {
            FileUtils.deleteDirectory(directory);
        }
    }

    protected  void deleteFile(String path) throws IOException {
        Files.deleteIfExists(new File(path).toPath());
    }


}
