package com.spark_tutorial.rdd.reduce;

import com.spark_tutorial.SparkExecutioner;
import lombok.extern.slf4j.Slf4j;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;

import java.util.Arrays;
import java.util.List;

@Slf4j
public class ReduceExample {

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.OFF);
        SparkConf conf = new SparkConf().setAppName("reduce").setMaster("local[*]");
        SparkExecutioner.doInJavaSparkContext(conf, sc -> {

            List<Integer> inputIntegers = Arrays.asList(1, 2, 3, 4, 5);
            JavaRDD<Integer> integerRdd = sc.parallelize(inputIntegers);

            Integer product = integerRdd.reduce((x, y) -> x * y);

            System.out.println("product is :" + product);
        });
    }
}
