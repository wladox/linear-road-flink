package com.github.wladox.state;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;

import com.github.wladox.model.Event;

/**
 * Enter some comments here...
 */
public class CountPositionReports extends RichMapFunction<Event, Integer> {

    private transient ValueState<Integer> sum;

    @Override
    public Integer map(Event positionReport) throws Exception {
        Integer newState = sum.value() + 1;
        sum.update(newState);
        return newState;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<Integer> descriptor = new ValueStateDescriptor<>(
                "totalCount", // the state name
                TypeInformation.of(new TypeHint<Integer>() {})); // type information;
        sum = getRuntimeContext().getState(descriptor);
    }
}
