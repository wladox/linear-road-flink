package com.github.wladox.function;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;

import com.github.wladox.model.Event;

/**
 *
 */
public class VehicleState extends RichMapFunction<Event, Event> {

  private transient ValueState<Event> previousPositionReport;
  private transient Counter positionReports;

  @Override
  public void open(Configuration config) {
    ValueStateDescriptor<Event> descriptor = new ValueStateDescriptor<>("vehicleState", TypeInformation.of(Event.class));
    previousPositionReport = getRuntimeContext().getState(descriptor);
    MetricGroup metricGroup = getRuntimeContext().getMetricGroup().addGroup("linear.road.flink");
    positionReports = metricGroup.counter("reports_total");
  }

  @Override
  public Event map(Event value) throws Exception {

    if (value.type.equals(0)) {

      positionReports.inc();

      Event e = previousPositionReport.value();
      if (e == null) {
        value.isCrossing = true;
        previousPositionReport.update(value);
        return value;
      }

      if (e.xWay.equals(value.xWay) && e.lane.equals(value.lane) && e.position.equals(value.position))
        value.samePositionCounter += e.samePositionCounter;

      if (value.samePositionCounter >= 4)
        value.isStopped = true;

      value.previousPosition = e.position;
      previousPositionReport.update(value);

      if (!e.segment.equals(value.segment) || e.lane == 4) { // if the car exited after last position report
        value.isCrossing = true;
        return value;
      }
    }

    return value;
  }
}
