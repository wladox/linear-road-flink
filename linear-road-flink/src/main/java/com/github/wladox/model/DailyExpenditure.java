package com.github.wladox.model;

import org.apache.flink.api.java.tuple.Tuple3;

/**
 * Created by root on 02.04.18.
 */
public class DailyExpenditure extends Tuple3<Integer, Integer, Integer[]> {

    public Integer xWay;
    public Integer vid;
    public Integer[] expenditures;



}
