package com.github.wladox.function;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;

import com.github.wladox.model.DailyExpenditure;
import com.github.wladox.model.Event;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

public class HistoricalTolls extends RichMapFunction<Event, String> {

  private transient Map<Tuple3<Integer, Integer, Integer>, String> myMap;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    myMap = new HashMap<>();
    ParameterTool params = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
    String historyPath = params.get("history", "");
    Files.readAllLines(Paths.get(historyPath))
      .forEach(s -> {
        String[] arr = s.split(",");
        DailyExpenditure exp = new DailyExpenditure(Integer.parseInt(arr[2]), Integer.parseInt(arr[0]), Integer.parseInt(arr[1]));
        myMap.put(exp, arr[3]);
      });
  }

  @Override
  public String map(Event value) throws Exception {
    DailyExpenditure key = new DailyExpenditure(value.xWay, value.vid, value.day);
    String toll = myMap.get(key);
    long emit = System.currentTimeMillis()-value.ingestTime;
    return String.format("3,%d,%d,%s,%s", value.time, emit, value.qid, toll);
  }

}
