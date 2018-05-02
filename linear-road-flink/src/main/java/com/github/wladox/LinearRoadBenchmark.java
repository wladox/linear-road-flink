package com.github.wladox;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.util.Collector;

import com.github.wladox.function.AccidentsState;
import com.github.wladox.function.SegmentStatistics;
import com.github.wladox.function.UpdateAccountBalance;
import com.github.wladox.function.VehicleState;
import com.github.wladox.model.AccidentNotification;
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

    if (args.length < 2) {
      System.err.println("Usage: --inputTopic <topicName> --outputTopic <topicName> --kafka <bootstrapServer>)");
    }

    ParameterTool parameter = ParameterTool.fromArgs(args);
    String inputTopic = parameter.get("inputTopic");
    String outputTopic = parameter.get("outputTopic");
    String broker = parameter.get("kafka");
    String groupId = "flink"+System.currentTimeMillis();

    final StreamExecutionEnvironment env  = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", broker);
    properties.setProperty("group.id", groupId);

    FlinkKafkaConsumer010<String> consumer = new FlinkKafkaConsumer010<>(inputTopic, new SimpleStringSchema(), properties);
    consumer.setStartFromEarliest();

    FlinkKafkaProducer010<String> producer = new FlinkKafkaProducer010<>(broker, outputTopic, new SimpleStringSchema());

    DataStream<String> stream = env.addSource(consumer);

    DataStream<Event> events = stream
      .filter(s -> !s.trim().isEmpty())
      .map(Event::parseFromString);

    DataStream<Event> positionReports = events
      .filter(s -> s.getType() == TYPE_POSITION_REPORT || s.getType() == TYPE_ACCOUNT_BALANCE_REQUEST)
      .process(new ProcessFunction<Event, Event>() {
        @Override
        public void processElement(Event value, Context ctx, Collector<Event> out) throws Exception {
          value.setIngestTime(ctx.timestamp());
          out.collect(value);
        }
      })
      .keyBy("vid")
      .map(new VehicleState());

    // TYPE-1 QUERY
    DataStream<Event> accidents = positionReports
      .keyBy(new KeySelector<Event, Integer>() {
      @Override
      public Integer getKey(Event value) throws Exception {
        return value.direction;
      }
    }).map(new AccidentsState());

    accidents
      .filter(e -> !e.accInSegment.equals(-1) && e.isCrossing && !e.lane.equals(4))
      .map(new MapFunction<Event, String>() {
      @Override
      public String map(Event value) throws Exception {
        long emit = System.currentTimeMillis() - value.ingestTime;
        return new AccidentNotification(value.time, 1.0, value.xWay, value.accInSegment, value.direction, value.vid).toString();
      }
    }).addSink(producer);


    KeySelector<Event, XwayDirSeg> xWayDirSeg = new KeySelector<Event, XwayDirSeg>() {
      @Override
      public XwayDirSeg getKey(Event value) throws Exception {
        return new XwayDirSeg(value.xWay, value.direction, value.segment);
      }
    };

    // TYPE-0 QUERY
    accidents
      .keyBy(xWayDirSeg)
      .map(new SegmentStatistics())
      .keyBy("vid")
      .map(new UpdateAccountBalance())
      .filter(s -> !s.isEmpty())
      .addSink(producer);


    // TYPE-3 QUERY IS NOT SUPPORTED DUE TO THE LACK OF SIDE INPUTS (FLIP-17)
    // https://cwiki.apache.org/confluence/display/FLINK/FLIP-17+Side+Inputs+for+DataStream+API)

    /*DataStream<TollHistory> history = env.readTextFile("/home/wladox/workspace/LRSparkApplication/data/car.dat.tolls.dat")
      .map((MapFunction<String, TollHistory>) value -> {
        String[] arr = value.split(",");
        return new TollHistory(Integer.parseInt(arr[0]), Integer.parseInt(arr[1]), Integer.parseInt(arr[2]), Integer.parseInt(arr[3]));
      });

    events.filter(s -> s.getType() == TYPE_DAILY_EXPENDITURE_REQUEST).join(history).where()*/


    env.execute();
  }

}
