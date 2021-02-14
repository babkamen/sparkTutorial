package com.spark_tutorial.pair_rdd.map_values;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;

import static com.spark_tutorial.SparkUtils.readResourcesFile;
import static com.spark_tutorial.rdd.airports.WordSparkUtils.readAirportsFile;
import static org.apache.spark.sql.functions.udf;

public class AirportsUppercaseProblem {

    /**
     * Create a Spark program to read the airport data from in/airports.text, generate a pair RDD with airport name
     * being the key and country name being the value. Then convert the country name to uppercase
     *
     * @return
     */
    public static Dataset<Row> process(SparkSession sparkSession, String inputFile) {
        final Dataset<Row> rdd = readAirportsFile(sparkSession, inputFile);
        rdd.createOrReplaceTempView("airports");
        String sql = readResourcesFile("/sql/airportToUppercase.sql");

        UserDefinedFunction toUpper = udf((UDF1<String, Object>) String::toUpperCase, DataTypes.StringType);
        sparkSession.udf().register("toUpper", toUpper);

        final Dataset<Row> dataset = rdd.sqlContext().sql(sql);
        dataset.show(10);
        dataset.printSchema();
        return dataset;
    }

}
