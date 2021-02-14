package com.spark_tutorial.rdd.airports;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.types.DataTypes.*;

public class WordSparkUtils {

    public static final String AIRPORT_ID = "Airport ID";
    public static final String NAME = "Name";
    public static final String CITY = "City";
    public static final String COUNTRY = "Country";
    public static final String FAA_CODE = "IATA/FAA code";
    public static final String ICAO_CODE = "ICAO Code";
    public static final String LATITUDE = "Latitude";
    public static final String LONGITUDE = "Longitude";
    public static final String ALTITUDE = "Altitude";
    public static final String TIMEZONE = "Timezone";
    public static final String DST = "DST";
    public static final String TIMEZONE_IN_OLSON_FORMAT = "Timezone in Olson format";

    private WordSparkUtils() {
    }

    public static Dataset<Row> readAirportsFile(SparkSession sc, String filepath) {
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
                .load(filepath);
    }

}
