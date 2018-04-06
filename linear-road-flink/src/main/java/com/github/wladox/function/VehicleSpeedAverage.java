package com.github.wladox.function;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

import com.github.wladox.model.Event;

/**
 *
 * @author Wladimir Postnikov
 */
public class VehicleSpeedAverage extends RichMapFunction<Event, Double> {

  /**
   * The ValueState handle. The first field is the count, the second field a running sum of speeds.
   */
  private transient ValueState<Tuple2<Integer, Integer>> state;

  @Override
  public void open(Configuration config) {
    ValueStateDescriptor<Tuple2<Integer, Integer>> descriptor = new ValueStateDescriptor<>(
        "minuteAverages",   // the state name
        TypeInformation.of(new TypeHint<Tuple2<Integer, Integer>>() {}),
        Tuple2.of(0, 0));       // default value of the state, if nothing was set
    state = getRuntimeContext().getState(descriptor);
  }


  @Override
  public Double map(Event value) throws Exception {
    // access the state value
    Tuple2<Integer, Integer> currentState = state.value();

    // update the count
    currentState.f0 += 1;
    currentState.f1 += value.getSpeed();

    // update the state
    state.update(currentState);

    // if the count reaches 2, emit the average and clear the state
    return currentState.f1 / Double.valueOf(currentState.f0);
  }
}
