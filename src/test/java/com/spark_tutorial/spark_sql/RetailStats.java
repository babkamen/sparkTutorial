package com.spark_tutorial.spark_sql;

import lombok.Data;

import java.math.BigDecimal;

@Data
public class RetailStats {

    private BigDecimal avgPricePerSqFtSum=BigDecimal.ZERO;
    private BigDecimal avgPricePerSqFt;
    private BigDecimal maxPrice = new BigDecimal("-1");
    private String location;
    private int count;

    public void incCount() {
        count++;
    }
}
