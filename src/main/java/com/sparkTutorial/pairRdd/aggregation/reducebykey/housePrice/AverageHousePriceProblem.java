package com.sparkTutorial.pairRdd.aggregation.reducebykey.housePrice;


import com.sparkTutorial.SparkUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Map;

/* Create a Spark program to read the house data from in/RealEstate.csv,
   output the average price for houses with different number of bedrooms.

The houses dataset contains a collection of recent real estate listings in San Luis Obispo county and
around it.

The dataset contains the following fields:
1. MLS: Multiple listing service number for the house (unique ID).
2. Location: city/town where the house is located. Most locations are in San Luis Obispo county and
northern Santa Barbara county (Santa Maria'rcutt, Lompoc, Guadelupe, Los Alamos), but there
some out of area locations as well.
3. Price: the most recent listing price of the house (in dollars).
4. Bedrooms: number of bedrooms.
5. Bathrooms: number of bathrooms.
6. Size: size of the house in square feet.
7. Price/SQ.ft: price of the house per square foot.
8. Status: type of sale. Thee types are represented in the dataset: Short Sale, Foreclosure and Regular.

Each field is comma separated.

Sample output:

   (3, 325000)
   (1, 266356)
   (2, 325000)
   ...

   3, 1 and 2 mean the number of bedrooms. 325000 means the average price of houses with 3 bedrooms is 325000.
 */
public class AverageHousePriceProblem {

    public static final String INPUT_FILE = "in/RealEstate.csv";

    private AverageHousePriceProblem() {}

    public static Map<Integer, BigDecimal> process() {
        final SparkSession sc = SparkUtils.setup();

        final Dataset<Row> dataset = sc.read()
                .format("com.databricks.spark.csv")
                .option("header", "true") // Use first line of all files as header
                .load(INPUT_FILE);
        final Map<Integer, Long> countByValue = dataset.toJavaRDD().map(row -> Integer.parseInt(row.getAs("Bedrooms"))).countByValue();
        System.out.println("Counts="+countByValue);
        final JavaPairRDD<Integer, BigDecimal> javaPairRDD = dataset.toJavaRDD()
                .mapToPair(row -> new Tuple2<>(Integer.parseInt(row.getAs("Bedrooms")), new BigDecimal((String) row.getAs("Price"))));

        final JavaPairRDD<Integer, BigDecimal> integerBigDecimalJavaPairRDD = javaPairRDD.reduceByKey(BigDecimal::add);

        System.out.println("Sums=" + integerBigDecimalJavaPairRDD.collectAsMap());
        final Map<Integer, BigDecimal> collect = integerBigDecimalJavaPairRDD.mapToPair(v1 -> {
            final Long count = countByValue.get(v1._1);
            final BigDecimal divisor = new BigDecimal(count);
            return new Tuple2<>(v1._1, v1._2.divide(divisor, RoundingMode.CEILING));
        }).collectAsMap();
        System.out.println("Averages=");
        collect.forEach((k, v) -> System.out.println("Row " + k + "=" + v));
        return collect;
    }
}
