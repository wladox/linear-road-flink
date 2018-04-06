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

import java.util.List;

/**
 *
 */
public class SegmentStatistics extends RichMapFunction<Event, Tuple3<Event, Integer, Integer>> {

  /**
   *  (totalPosRep, totalSpeeds, NOV)
   */
  private transient MapState<Integer, Tuple3<Integer, Integer, Integer>> segmentStatistics;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    MapStateDescriptor<Integer, Tuple3<Integer, Integer, Integer>> descriptor = new MapStateDescriptor<>("speedPerSegment",
      TypeInformation.of(Integer.class), TypeInformation.of(new TypeHint<Tuple3<Integer, Integer, Integer>>() {
    }));
    segmentStatistics = getRuntimeContext().getMapState(descriptor);
  }

  @Override
  public Tuple3<Event, Integer, Integer> map(Event value) throws Exception {

    if (segmentStatistics.contains(value.minute)) {
      Tuple3<Integer, Integer, Integer> prev = segmentStatistics.get(value.minute);
      prev.f0 += 1;
      prev.f1 += value.speed;
      prev.f2 += value.isCrossing ? 1 : 0;
      segmentStatistics.put(value.minute, prev);
    } else {
      segmentStatistics.put(value.minute, Tuple3.of(1, value.speed, 1));
    }

    int totalCount = 0;
    int totalSpeed = 0;
    for (int i = 1; i <= 5; i++) {
      if (segmentStatistics.contains(value.minute-i)) {
        Tuple3<Integer, Integer, Integer> v = segmentStatistics.get(value.minute - i);
        totalCount += v.f0;
        totalSpeed += v.f1;
      }
    }

    // return LAV for last 5 minutes

    Tuple3<Integer, Integer, Integer> t = segmentStatistics.get(value.minute);

    double lav = totalCount != 0 ? (double) totalSpeed / totalCount : 0;

    return Tuple3.of(value, t.f2, Double.valueOf(Math.ceil(lav)).intValue());

  }
}
