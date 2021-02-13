package com.sparkTutorial.rdd.airports;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public abstract class AbstractAirportsProblemCSV extends AbstractAirportsProblem<Dataset<Row>> {

    @Override
    protected void saveToFile(Dataset<Row> selectAndFilter, String partsFolderPath) {
        selectAndFilter.coalesce(1)
                .write()
                .format("com.databricks.spark.csv")
                .option("header", "true")
                .save(partsFolderPath);
    }
}
