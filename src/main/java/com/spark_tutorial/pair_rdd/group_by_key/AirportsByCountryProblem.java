package com.spark_tutorial.pair_rdd.group_by_key;

import com.spark_tutorial.SparkUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import static com.spark_tutorial.rdd.airports.WordSparkUtils.*;


public class AirportsByCountryProblem {

    public JavaPairRDD<String, Iterable<String>> processData(String inputFilepath) {
        final SparkSession sc = SparkUtils.getOrCreateSession();
        final Dataset<Row> rdd = readAirportsFile(sc, inputFilepath);
        //output the the list of the names of the airports located in each country.
        return rdd.toJavaRDD()
                .mapToPair(row -> new Tuple2<String, String>(row.getAs(COUNTRY), row.getAs(NAME)))
                .groupByKey();
    }
}
