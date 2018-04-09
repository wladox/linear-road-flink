package com.github.wladox.function;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;

import com.github.wladox.model.AccountBalance;
import com.github.wladox.model.AccountBalanceResponse;
import com.github.wladox.model.Event;
import com.github.wladox.model.TollNotification;

public class UpdateAccountBalance extends RichMapFunction<Event, String> {

  private transient ValueState<AccountBalance> state;
  private transient Counter type0;
  private transient Counter type2;


  @Override
  public void open(Configuration parameters) throws Exception {
    state = getRuntimeContext().getState(new ValueStateDescriptor<>("accountBalance", TypeInformation.of(AccountBalance.class)));
    MetricGroup metricGroup = getRuntimeContext().getMetricGroup().addGroup("linear.road.flink");
    type0 = metricGroup.counter("type0");
    type2 = metricGroup.counter("type2");

  }

  @Override
  public String map(Event event) throws Exception {

    AccountBalance balance = state.value();

    if (balance == null)
      balance = new AccountBalance(0, (short) 0,0);

    if (event.type == 0) {
      // TOLL NOTIFICATION
      if (event.lane.equals(4)) {
        // car is leaving the expressway
        balance.balance += balance.currentToll;
        balance.currentToll = 0;
        balance.lastUpdated = event.time;
        state.update(balance);
        return "";
      } else {
        Double nov = (double) event.nov;
        Integer lav = event.lav;
        boolean isAccident = !event.accInSegment.equals(-1);

        Long toll = 0L;
        if (lav < 40 && nov > 50 && !isAccident) {
          toll = Math.round(2 * Math.pow(nov , 2));
        }
        balance.balance     += balance.currentToll; // charge toll of previous segment
        balance.currentToll = toll.intValue();      // set toll for current segment
        balance.lastUpdated = event.time;           // set time of last update
        state.update(balance);
        type0.inc();
        return new TollNotification(event.vid, event.time, 1, lav, toll.intValue()).toString();
      }

    } else {
      // ACCOUNT BALANCE REQUEST
      type2.inc();
      return new AccountBalanceResponse(event.time, 1, balance.lastUpdated, event.qid, balance.balance).toString();
    }

  }

}
