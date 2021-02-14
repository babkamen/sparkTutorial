package com.spark_tutorial.spark_sql;

import com.opencsv.bean.CsvBindByName;
import lombok.Data;

import java.math.BigDecimal;

@Data
public class RetailRecord{
    @CsvBindByName(column = "Price",required = true)
    private BigDecimal price;

    @CsvBindByName(column = "Location",required = true)
    private String location;

    @CsvBindByName(column = "Price SQ Ft",required = true)
    private BigDecimal pricePerSqFt;
	// Getters and Setters (Omitted for brevity)
}