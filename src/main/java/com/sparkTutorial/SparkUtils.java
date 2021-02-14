package com.sparkTutorial;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.SparkSession;

public class SparkUtils {

    public static SparkSession setup() {
        Logger.getLogger("org").setLevel(Level.ERROR);
        return SparkSession.builder()
                .master("local[*]")
                .appName("app")
                .getOrCreate();
    }
}
