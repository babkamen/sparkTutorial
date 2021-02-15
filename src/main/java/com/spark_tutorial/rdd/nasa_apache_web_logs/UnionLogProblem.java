package com.spark_tutorial.rdd.nasa_apache_web_logs;

import org.apache.commons.io.FileUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.io.IOException;

public class UnionLogProblem {


    /**
     * "in/nasa_19950701.tsv" file contains 10000 log lines from one of NASA's apache server for July 1st, 1995.
     * "in/nasa_19950801.tsv" file contains 10000 log lines for August 1st, 1995
     * Create a Spark program to generate a new RDD which contains the log lines from both July 1st and August 1st,
     * take a 0.1 sample of those log lines and save it to "out/sample_nasa_logs.tsv" file.
     * <p>
     * Keep in mind, that the original log files contains the following header lines.
     * host	logname	time	method	url	response	bytes
     * <p>
     * Make sure the head lines are removed in the resulting RDD.
     */
    public static void process(SparkSession ss, String outputFile, String... inputFiles) throws IOException {
        FileUtils.deleteDirectory(new File(outputFile));
        JavaRDD<Row> result = null;

        for (String inputFile : inputFiles) {
            final JavaRDD<Row> df = ss.read()
                    .format("com.databricks.spark.csv")
                    .option("inferSchema", "true")
                    .option("header", "true")
                    .option("delimiter", "\t")
                    .load(inputFile).toJavaRDD();
            if (result == null) {
                result = df;
            } else {
                result = result.union(df);
            }
        }
        assert result != null;
        result.sample(false, 0.1).saveAsTextFile(outputFile);
    }
}
