package com.spark_tutorial.pair_rdd.filter;

import com.spark_tutorial.rdd.airports.AbstractAirportsProblem;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import static com.spark_tutorial.rdd.airports.WordSparkUtils.*;

/**
 * Create a Spark program to read the airport data from in/airports.text;
 * generate a pair RDD with airport name being the key and country name being the value.
 * Then remove all the airports which are located in United States and output the pair RDD to out/airports_not_in_usa_pair_rdd.text
 */
@Slf4j
public class AirportsNotInUsaProblem extends AbstractAirportsProblem<JavaPairRDD<Object, Object>> {


    @Override
    protected void renamePartsToFilename(String partsFolder, String filepath) throws IOException {
        final File dir = new File(partsFolder);
        System.out.println(dir.getAbsolutePath());
        final File[] files = new File(partsFolder).listFiles((file, s) -> s.startsWith("part-") && !s.contains("."));
        assert files != null && files.length > 0;
        System.out.println(Arrays.toString(files));
        FileUtils.moveFile(files[0], new File(filepath));
    }

    @Override
    protected void saveToFile(JavaPairRDD<Object, Object> rdd, String outputFile) {
        rdd.saveAsTextFile(outputFile);
    }

    @Override
    protected JavaPairRDD<Object, Object> selectAndFilter(Dataset<Row> rdd) {
        return rdd.select(NAME, CITY).filter(COUNTRY + "<>'United States'")
                .toJavaRDD().mapToPair(row -> new Tuple2<>(row.get(0), row.get(1)));
    }
}
