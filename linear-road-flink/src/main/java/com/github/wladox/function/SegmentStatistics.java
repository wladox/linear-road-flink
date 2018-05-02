package com.github.wladox.function;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;

import com.github.wladox.model.Event;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 *
 */
public class SegmentStatistics extends RichMapFunction<Event, Event> {

  /**
   *  (totalPosRep, totalSpeeds, NOV)
   */
  private transient MapState<Integer, Tuple3<Integer, Integer, Set<Integer>>> segmentStatistics;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    MapStateDescriptor<Integer, Tuple3<Integer, Integer, Set<Integer>>> descriptor = new MapStateDescriptor<>("speedPerSegment",
      TypeInformation.of(Integer.class), TypeInformation.of(new TypeHint<Tuple3<Integer, Integer, Set<Integer>>>() {
    }));
    segmentStatistics = getRuntimeContext().getMapState(descriptor);
  }

  @Override
  public Event map(Event value) throws Exception {

    if (value.type == 0) {
      if (segmentStatistics.contains(value.minute)) {
        Tuple3<Integer, Integer, Set<Integer>> prev = segmentStatistics.get(value.minute);
        prev.f0 += 1;
        prev.f1 += value.speed;
        prev.f2.add(value.vid);
        segmentStatistics.put(value.minute, prev);
      } else {
        Set<Integer> cars = new HashSet<>();
        cars.add(value.vid);
        segmentStatistics.put(value.minute, Tuple3.of(1, value.speed, cars));
      }

      int totalCount = 0;
      int totalSpeed = 0;
      for (int i = 1; i <= 5; i++) {
        if (segmentStatistics.contains(value.minute-i)) {
          Tuple3<Integer, Integer, Set<Integer>> v = segmentStatistics.get(value.minute - i);
          totalCount += v.f0;
          totalSpeed += v.f1;
        }
      }
      // return LAV for last 5 minutes
      if (value.minute > 1) {
        Tuple3<Integer, Integer, Set<Integer>> t = segmentStatistics.get(value.minute-1);
        int lav = totalCount > 0 ? (Math.round(totalSpeed / ((float) totalCount))) : 0;
        if (t != null)
          value.nov = t.f2.size();
        else
          value.nov = 0;
        value.lav = lav;
      } else {
        value.nov = 0;
        value.lav = 0;
      }
    }

    return value;

  }
}
