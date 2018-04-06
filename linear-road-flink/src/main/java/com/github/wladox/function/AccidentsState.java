package com.github.wladox.function;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;

import com.github.wladox.model.Accident;
import com.github.wladox.model.Event;

/**
 * Accidents on an Xway, Direction.
 */
public class AccidentsState extends RichMapFunction<Event, Event> {

  private ValueState<Accident[]> accidents;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    accidents = getRuntimeContext().getState(
      new ValueStateDescriptor<>("accidents", TypeInformation.of(new TypeHint<Accident[]>() {}))
    );
    Accident[] initialState = new Accident[100];
    accidents.update(initialState);
  }

  @Override
  public Event map(Event event) throws Exception {
    Accident[] state = accidents.value();
    if (event.isStopped) {
      if (state[event.segment] != null) {
        state[event.segment].secondVehicle = event.vid;
        state[event.segment].created = event.time;
      } else {
        state[event.segment] = new Accident(event.vid, -1, (short) -1);
      }
      accidents.update(state);
    } else {
      // clear accident
      int segment = event.previousPosition / 5280;
      if (state[segment] != null && (state[segment].firstVehicle.equals(event.vid) || state[segment].secondVehicle.equals(event.vid))){
        state[segment].cleared = event.minute;
        accidents.update(state);
      }
    }

    event.accInSegment = findAccidentSegment(state, event);

    return event;
  }

  private int findAccidentSegment(Accident[] accidents, Event event) {

    int from;
    int to;
    if (event.direction.equals(0)) {
      from = event.segment;
      to = event.segment + 4;
    } else {
      from = event.segment - 4;
      to = event.segment;
    }

    for (int i = from; i <= to; i++) {
      if (accidents[i] != null &&
        accidents[i].created < event.minute && (accidents[i].created.equals(-1) || event.minute < accidents[i].created)) {
        return i;
      }
    }
    return -1;
  }


}
