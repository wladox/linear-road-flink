package com.github.wladox.function;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

import com.github.wladox.model.DailyExpenditure;
import com.github.wladox.model.Event;

public class HistoricalTolls extends RichMapFunction<Event, DailyExpenditure> {



  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);


  }

  @Override
  public DailyExpenditure map(Event value) throws Exception {
    return null;
  }
}
