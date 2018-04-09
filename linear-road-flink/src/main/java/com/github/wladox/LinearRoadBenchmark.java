package com.github.wladox;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;

import com.github.wladox.function.SegmentStatistics;
import com.github.wladox.function.UpdateAccountBalance;
import com.github.wladox.function.VehicleState;
import com.github.wladox.model.Event;
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

    if (args.length < 1) {
      System.err.println("Usage: --topic <topicName> (--parallelism <levelOfParallelism>)");
    }

    ParameterTool parameter = ParameterTool.fromArgs(args);
    String topicName = parameter.get("topic");
    int parallelism = parameter.getInt("parallelism", 2);

    final StreamExecutionEnvironment env  = StreamExecutionEnvironment.getExecutionEnvironment();
    //env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

    ExecutionConfig config = env.getConfig();
    config.setParallelism(parallelism);

    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", "localhost:9092");
    properties.setProperty("group.id", "linear-road-flink");

    FlinkKafkaConsumer010<String> consumer = new FlinkKafkaConsumer010<>(topicName, new SimpleStringSchema(), properties);
    //consumer.setStartFromEarliest();

    DataStream<String> stream = env.addSource(consumer);

    /*DataStream<TollHistory> history = env.readTextFile("/home/wladox/workspace/LRSparkApplication/data/car.dat.tolls.dat")
      .map((MapFunction<String, TollHistory>) value -> {
        String[] arr = value.split(",");
        return new TollHistory(Integer.parseInt(arr[0]), Integer.parseInt(arr[1]), Integer.parseInt(arr[2]), Integer.parseInt(arr[3]));
      });*/

    DataStream<Event> events = stream
      .filter(s -> !s.trim().isEmpty())
      .map(Event::parseFromString);

    DataStream<Event> positionReports = events
      .filter(s -> s.getType() == TYPE_POSITION_REPORT || s.getType() == TYPE_ACCOUNT_BALANCE_REQUEST);

    // TYPE-2 QUERY
    //DataStream balanceRequests = events.filter(s -> s.getType() == TYPE_ACCOUNT_BALANCE_REQUEST);

    /*KeySelector<Event, Tuple4<Byte, Byte, Byte, Integer>> xWayDirSegMin = new KeySelector<Event, Tuple4<Byte, Byte, Byte, Integer>>() {
      @Override
      public Tuple4<Byte, Byte, Byte, Integer> getKey(Event value) throws Exception {
        return new Tuple4<>(value.xWay, value.direction, value.segment, value.minute);
      }
    };*/

    KeySelector<Event, XwayDirSeg> xWayDirSeg = new KeySelector<Event, XwayDirSeg>() {
      @Override
      public XwayDirSeg getKey(Event value) throws Exception {
        return new XwayDirSeg(value.xWay, value.direction, value.segment);
      }
    };

    //KeySelector<Event, Integer> vehicleKey = (KeySelector<Event, Integer>) value -> value.vid;

    // TYPE-0 QUERY
    positionReports
      .keyBy("vid")
      .map(new VehicleState())
      .keyBy(xWayDirSeg)
      .map(new SegmentStatistics())
      .filter((FilterFunction<Event>) value -> value.isCrossing || value.type == TYPE_ACCOUNT_BALANCE_REQUEST)
      .keyBy("vid")
      .map(new UpdateAccountBalance())
      .filter(s -> !cds.isEmpty())
      .addSink(new FlinkKafkaProducer010<>("localhost:9092", "type-02-flink", new SimpleStringSchema()));


    // TYPE-3 QUERY IS NOT SUPPORTED DUE TO THE LACK OF SIDE INPUTS (FLIP-17)
    // https://cwiki.apache.org/confluence/display/FLINK/FLIP-17+Side+Inputs+for+DataStream+API)
    //events.filter(s -> s.getType() == TYPE_DAILY_EXPENDITURE_REQUEST);


  env.execute();
  }

}
