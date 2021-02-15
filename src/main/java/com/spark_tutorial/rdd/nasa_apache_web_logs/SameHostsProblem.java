package com.spark_tutorial.rdd.nasa_apache_web_logs;

import org.apache.commons.io.FileUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.io.IOException;

public class SameHostsProblem {


    /**
     * "in/nasa_19950701.tsv" file contains 10000 log lines from one of NASA's apache server for July 1st, 1995.
     * "in/nasa_19950801.tsv" file contains 10000 log lines for August 1st, 1995
     * Create a Spark program to generate a new RDD which contains the hosts which are accessed on BOTH days.
     * Save the resulting RDD to "out/nasa_logs_same_hosts.csv" file.
     * <p>
     * Example output:
     * vagrant.vf.mmc.com
     * www-a1.proxy.aol.com
     * .....
     * <p>
     * Keep in mind, that the original log files contains the following header lines.
     * host	logname	time	method	url	response	bytes
     * <p>
     * Make sure the head lines are removed in the resulting RDD.
     */
    public static void process(SparkSession sparkSession, String outputPath, String... inputFiles) throws IOException {
        FileUtils.deleteDirectory(new File(outputPath));
        JavaRDD<Object> result = null;

        for (String inputFile : inputFiles) {
            final JavaRDD<Object> df = sparkSession.read()
                    .format("com.databricks.spark.csv")
                    .option("inferSchema", "true")
                    .option("header", "true")
                    .option("delimiter", "\t")
                    .load(inputFile)
                    .toJavaRDD()
                    .map(e -> e.getAs("host"));
            if (result == null) {
                result = df;
            } else {
                result = result.intersection(df);
            }
        }
        assert result != null;
        System.out.println("CombinedDF=" + result.top(3));
        result.saveAsTextFile(outputPath);
    }
}
