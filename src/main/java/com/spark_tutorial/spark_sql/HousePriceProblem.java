package com.spark_tutorial.spark_sql;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

public class HousePriceProblem {

    public static final String VIEW_NAME = "real";
    public static final String AVG_PRICE_COLUMN_NAME = "avgPricePerSqFt";
    public static final String MAX_PRICE_COLUMN_NAME = "maxPrice";

    /* Create a Spark program to read the house data from in/RealEstate.csv,
               group by location, aggregate the average price per SQ Ft and max price, and sort by average price per SQ Ft.

            The houses dataset contains a collection of recent real estate listings in San Luis Obispo county and
            around it. 

            The dataset contains the following fields:
            1. MLS: Multiple listing service number for the house (unique ID).
            2. Location: city/town where the house is located. Most locations are in San Luis Obispo county and
            northern Santa Barbara county (Santa Maria­Orcutt, Lompoc, Guadelupe, Los Alamos), but there
            some out of area locations as well.
            3. Price: the most recent listing price of the house (in dollars).
            4. Bedrooms: number of bedrooms.
            5. Bathrooms: number of bathrooms.
            6. Size: size of the house in square feet.
            7. Price/SQ.ft: price of the house per square foot.
            8. Status: type of sale. Thee types are represented in the dataset: Short Sale, Foreclosure and Regular.

            Each field is comma separated.

            Sample output:

            +----------------+-----------------+----------+
            |        Location| avg(Price SQ Ft)|max(Price)|
            +----------------+-----------------+----------+
            |          Oceano|           1145.0|   1195000|
            |         Bradley|            606.0|   1600000|
            | San Luis Obispo|            459.0|   2369000|
            |      Santa Ynez|            391.4|   1395000|
            |         Cayucos|            387.0|   1500000|
            |.............................................|
            |.............................................|
            |.............................................|

             */
    public static Dataset<Row> process(SparkSession sparkSession, String inputFile) {
        //Create a Spark program to read the house data from in/RealEstate.csv,
        //       group by location, aggregate the average price per SQ Ft and max price, and sort by average price per SQ Ft.
        final SQLContext sqlContext = sparkSession.sqlContext();

        final Dataset<Row> dataset = sqlContext.read()
                .option("header", "true") // Use first line of all files as header
                .csv(inputFile);
        dataset.createOrReplaceTempView(VIEW_NAME);
        return dataset.sqlContext()
                .sql(String.format("Select max(Price) as %s,avg(Price/`Price SQ Ft`) as %s,Location  from %s GROUP BY Location ORDER BY %s",
                        MAX_PRICE_COLUMN_NAME, AVG_PRICE_COLUMN_NAME, VIEW_NAME, AVG_PRICE_COLUMN_NAME));
    }
}
