package com.github.wladox;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * Hello world!
 *
 */
public class App {
    public static void main( String[] args ) throws Exception {

        String s = "0,3,4";
        String[] arr = s.split(",");
        Byte lane = Byte.parseByte(arr[2]);
        System.out.println(lane);
        System.out.println(lane == 4);


        Integer a = 4;
        a += 1;
        System.out.println(a);
    }
}
