package com.spark_tutorial;

import com.spark_tutorial.spark_sql.HousePriceProblem;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.nio.charset.StandardCharsets;
import java.util.Scanner;

public class SparkUtils {

    public static final String LOCAL = "local[*]";

    private SparkUtils() {
    }

    public static SparkConf createConf() {
        return new SparkConf().setAppName("Local").setMaster(LOCAL);
    }

    public static SparkSession getOrCreateSession() {
        Logger.getLogger("org").setLevel(Level.ERROR);
        return SparkSession.builder()
                .master(LOCAL)
                .appName("app")
                .getOrCreate();
    }

    public static JavaSparkContext getOrCreateJavaSparkContext() {
        Logger.getLogger("org").setLevel(Level.ERROR);
        return new JavaSparkContext(getOrCreateSession().sparkContext());
    }

    public static String readResourcesFile(String path) {
        return new Scanner(SparkUtils.class.getResourceAsStream(path),
                StandardCharsets.UTF_8).useDelimiter("\\A").next();
    }

}
