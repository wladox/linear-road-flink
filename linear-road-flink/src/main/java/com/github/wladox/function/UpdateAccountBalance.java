package com.github.wladox.function;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;

import com.github.wladox.model.AccountBalance;
import com.github.wladox.model.AccountBalanceResponse;
import com.github.wladox.model.Event;
import com.github.wladox.model.TollNotification;

public class UpdateAccountBalance extends RichMapFunction<Event, String> {

  private transient ValueState<AccountBalance> state;

  private transient Counter type0;
  private transient Counter type2;
  private transient long response0;
  private transient long response2;


  @Override
  public void open(Configuration parameters) throws Exception {
    state = getRuntimeContext().getState(new ValueStateDescriptor<>("accountBalance", TypeInformation.of(AccountBalance.class)));
    MetricGroup metricGroup = getRuntimeContext().getMetricGroup().addGroup("linear.road.flink");
    type0 = metricGroup.counter("type0");
    type2 = metricGroup.counter("type2");
    metricGroup.gauge("type0response", (Gauge<Long>) () -> response0);
    metricGroup.gauge("type2response", (Gauge<Long>) () -> response2);
  }

  @Override
  public String map(Event event) throws Exception {

    AccountBalance balance = state.value();

    if (balance == null)
      balance = new AccountBalance(0, (short) 0,0);

    if (event.type == 0) {
      // TOLL NOTIFICATION
      if (event.isCrossing) {
        if (event.lane == 4) {
          // car is leaving the expressway
          balance.balance += balance.currentToll;
          balance.currentToll = 0;
          balance.lastUpdated = event.time;
          state.update(balance);
          return "";
        } else {
          if (event.vid == 20640 && event.time == 1080)
            System.out.println();
          Double nov = (double) event.nov;
          Integer lav = event.lav;
          boolean isAccident = !event.accInSegment.equals(-1);

          int toll = 0;
          if (nov > 50 && lav < 40 && !isAccident) {
            toll = (int) (2 * Math.pow(nov - 50 , 2));
          }

          balance.balance     += balance.currentToll; // charge toll of previous segment
          balance.currentToll = toll;

          balance.lastUpdated = event.time;           // set time of last update
          state.update(balance);
          long emit = System.currentTimeMillis() - event.ingestTime;
          response0 = emit;
          type0.inc();
          return new TollNotification(event.vid, event.time, 1.0, lav, toll).toString();
        }
      } else {
        // update time of account balance
        balance.lastUpdated = event.time;
        state.update(balance);
        return "";
      }
    } else {
      // ACCOUNT BALANCE REQUEST
      long emit = System.currentTimeMillis() - event.ingestTime;
      response2 = emit;
      type2.inc();
      return new AccountBalanceResponse(event.time, 1.0, balance.lastUpdated, event.qid, balance.balance).toString();
    }

  }

}
