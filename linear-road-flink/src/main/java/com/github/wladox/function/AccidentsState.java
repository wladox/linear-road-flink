package com.github.wladox.function;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;

import com.github.wladox.model.Accident;
import com.github.wladox.model.Event;

/**
 * Accidents on an Xway, Direction.
 */
public class AccidentsState extends RichMapFunction<Event, Event> {

  private ValueState<Accident[]> accidents;
  private transient Counter accidentNotifications;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    accidents = getRuntimeContext().getState(
      new ValueStateDescriptor<>("accidents", TypeInformation.of(new TypeHint<Accident[]>() {}))
    );
    MetricGroup metricGroup = getRuntimeContext().getMetricGroup().addGroup("linear.road.flink");
    accidentNotifications = metricGroup.counter("type_1_total");
  }

  @Override
  public Event map(Event event) throws Exception {

    if (event.getType() == 0) {
      Accident[] state = accidents.value();

      if (state == null)
        state = new Accident[100];

      if (event.isStopped) {
        if (state[event.segment] != null) {
          if (state[event.segment].secondVehicle == -1) {
            state[event.segment].secondVehicle = event.vid;
            state[event.segment].created = event.minute;
            System.out.println("ACCIDENT OCCURED: min=" + event.minute + " seg=" + event.segment);
          }
        } else {
          state[event.segment] = new Accident(event.vid, -1);
        }
        accidents.update(state);
      } else {
        // clear accident
        int segment = event.previousPosition / 5280;
        if (state[segment] != null && state[segment].cleared == -1 && (state[segment].firstVehicle.equals(event.vid) || state[segment].secondVehicle.equals(event.vid))){
          state[segment].cleared = event.minute;
          accidents.update(state);
        }
      }

      event.accInSegment = findAccidentSegment(state, event);
      if (!event.accInSegment.equals(-1))
        accidentNotifications.inc();
    }

    return event;
  }

  private int findAccidentSegment(Accident[] accidents, Event event) {

    int from;
    int to;
    if (event.direction.equals(0)) {
      from = event.segment;
      to = Math.min(event.segment + 4, 99);

    } else {
      from = Math.max(event.segment - 4, 0);
      to = event.segment;
    }

    for (int i = from; i <= to; i++) {
      Accident a = accidents[i];
      if (a != null && ((event.minute >= a.created+1 && a.cleared.equals(-1)) || (event.minute >= a.created+1 && event.minute <= a.cleared))) {
        return i;
      }
    }
    return -1;
  }


}
