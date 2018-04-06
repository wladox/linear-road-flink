package com.github.wladox;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

import com.github.wladox.function.SegmentStatistics;
import com.github.wladox.function.VehicleState;
import com.github.wladox.model.Event;
import com.github.wladox.model.TollHistory;
import com.github.wladox.model.TollNotification;
import com.github.wladox.model.XwayDirSeg;

import java.util.Properties;

/**
 *  The entry point for running the benchmark.
 */
public class LinearRoadBenchmark {

  private static final int TYPE_POSITION_REPORT            = 0;
  private static final int TYPE_ACCOUNT_BALANCE_REQUEST    = 2;
  private static final int TYPE_DAILY_EXPENDITURE_REQUEST  = 3;

  public static void main(String[] args) throws Exception {

    final StreamExecutionEnvironment env  = StreamExecutionEnvironment.getExecutionEnvironment();
    //env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", "localhost:9092");
    properties.setProperty("group.id", "linear-road-flink");

    FlinkKafkaConsumer010<String> consumer = new FlinkKafkaConsumer010<>("linear-road-flink-9", new SimpleStringSchema(), properties);
    //consumer.setStartFromEarliest();

    DataStream<String> stream = env.addSource(consumer);

    DataStream<TollHistory> history = env.readTextFile("/home/wladox/workspace/LRSparkApplication/data/car.dat.tolls.dat")
      .map((MapFunction<String, TollHistory>) value -> {
        String[] arr = value.split(",");
        return new TollHistory(Integer.parseInt(arr[0]), Integer.parseInt(arr[1]), Integer.parseInt(arr[2]), Integer.parseInt(arr[3]));
      });

    DataStream<Event> events = stream
      .filter(s -> !s.trim().isEmpty())
      .map(Event::parseFromString);

    DataStream<Event> positionReports = events
      .filter(s -> s.getType() == TYPE_POSITION_REPORT);

    // TYPE-2 QUERY
    DataStream balanceRequests = events.filter(s -> s.getType() == TYPE_ACCOUNT_BALANCE_REQUEST);

    KeySelector<Event, Tuple4<Byte, Byte, Byte, Integer>> xWayDirSegMin = new KeySelector<Event, Tuple4<Byte, Byte, Byte, Integer>>() {
      @Override
      public Tuple4<Byte, Byte, Byte, Integer> getKey(Event value) throws Exception {
        return new Tuple4<>(value.xWay, value.direction, value.segment, value.minute);
      }
    };

    KeySelector<Event, XwayDirSeg> xWayDirSeg = new KeySelector<Event, XwayDirSeg>() {
      @Override
      public XwayDirSeg getKey(Event value) throws Exception {
        return new XwayDirSeg(value.xWay, value.direction, value.segment);
      }
    };

    KeySelector<Event, Integer> vehicleKey = (KeySelector<Event, Integer>) value -> value.vid;

    // TYPE-0 QUERY
    positionReports
      .keyBy(vehicleKey)
      .map(new VehicleState())
      .keyBy(xWayDirSeg)
      .map(new SegmentStatistics())
      .filter((FilterFunction<Tuple3<Event, Integer, Integer>>) value -> value.f0.isCrossing)
      .map((MapFunction<Tuple3<Event, Integer, Integer>, String>) value -> {

        Double nov = Double.valueOf(value.f1);
        Integer lav = value.f2;
        boolean isAccident = !value.f0.accInSegment.equals(-1);

        Long toll = 0L;
        if (lav < 40 && nov > 50 && !isAccident) {
          toll = Math.round(2 * Math.pow(nov , 2));
        }

        return new TollNotification(value.f0.vid.toString(), value.f0.time.toString(), "1", String.valueOf(lav), toll.toString()).toString();
      });
    //.addSink(new FlinkKafkaProducer010<>("localhost:9092", "type-0-flink-9", new SimpleStringSchema()));


    // CALCULATE MINUTE AVERAGES
    /*positionReports
      .keyBy(xWayDirSegMin)
      .map(new VehicleSpeedAverage());*/

    // CALCULATE NUMBER OF VEHICLES ON SEGMENT PER MINUTE
    /*positionReports
      .keyBy(xWayDirSegMin)
      .timeWindow(Time.seconds(60));*/


    // TYPE-3 QUERY IS NOT SUPPORTED DUE TO THE LACK OF SIDE INPUTS (FLIP-17)
    // https://cwiki.apache.org/confluence/display/FLINK/FLIP-17+Side+Inputs+for+DataStream+API)
    events.filter(s -> s.getType() == TYPE_DAILY_EXPENDITURE_REQUEST);


  env.execute();

  }

}
