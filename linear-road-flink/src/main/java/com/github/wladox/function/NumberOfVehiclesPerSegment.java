package com.github.wladox.function;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

import com.github.wladox.model.Event;

/**
 *
 */
public class NumberOfVehiclesPerSegment extends RichMapFunction<Event, Tuple2<Event, Integer>> {

  private transient MapState<Integer, Integer> segmentCounts;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    MapStateDescriptor<Integer, Integer> descriptor = new MapStateDescriptor<>("novPerSegment", Integer.class, Integer.class);
    segmentCounts = getRuntimeContext().getMapState(descriptor);
  }

  @Override
  public Tuple2<Event, Integer> map(Event value) throws Exception {

    if (value.isCrossing) {
      if (segmentCounts.contains(value.minute)) {
        Integer newCount = segmentCounts.get(value.minute) + 1;
        segmentCounts.put(value.minute, newCount);
      } else {
        segmentCounts.put(value.minute, 1);
      }
    }

    Integer countPrevMinute = segmentCounts.get(value.minute - 1);

    return new Tuple2<>(value, countPrevMinute != null ? countPrevMinute : 0);

  }
}
