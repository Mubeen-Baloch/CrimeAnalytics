package edu.nu.crimeanalytics.topology;

import edu.nu.crimeanalytics.bolts.AlertBolt;
import edu.nu.crimeanalytics.bolts.AnomalyBolt;
import edu.nu.crimeanalytics.bolts.DistrictBolt;
import edu.nu.crimeanalytics.bolts.ParseBolt;
import edu.nu.crimeanalytics.bolts.RawEventBolt;
import edu.nu.crimeanalytics.bolts.WindowCountBolt;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.kafka.spout.FirstPollOffsetStrategy;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;

public class CrimeAnalyticsTopology {
  public static void main(String[] args) throws Exception {
    String brokers = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092");
    String topic = System.getenv().getOrDefault("KAFKA_TOPIC", "crime_events");
    String name = args.length > 0 ? args[0] : System.getenv().getOrDefault("TOPOLOGY_NAME", "crime-analytics");
    int windowSeconds = Integer.parseInt(System.getenv().getOrDefault("WINDOW_SIZE_SECONDS", "300"));
    int slideSeconds = Integer.parseInt(System.getenv().getOrDefault("SLIDE_INTERVAL_SECONDS", "60"));

    KafkaSpoutConfig<String, String> kafkaConfig = KafkaSpoutConfig.builder(brokers, topic)
        .setProp(ConsumerConfig.GROUP_ID_CONFIG, "crime-storm-consumer")
        .setProp(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
        .setProp(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
        .setFirstPollOffsetStrategy(FirstPollOffsetStrategy.LATEST)
        .build();

    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout("kafka-spout", new KafkaSpout<>(kafkaConfig), 1);
    builder.setBolt("parse-bolt", new ParseBolt(), 2).shuffleGrouping("kafka-spout");
    builder.setBolt("raw-event-bolt", new RawEventBolt(), 1).shuffleGrouping("parse-bolt");
    builder.setBolt("district-bolt", new DistrictBolt(), 2).fieldsGrouping("parse-bolt", new Fields("district"));
    builder.setBolt("window-bolt", new WindowCountBolt()
        .withWindow(new BaseWindowedBolt.Duration(windowSeconds, java.util.concurrent.TimeUnit.SECONDS),
            new BaseWindowedBolt.Duration(slideSeconds, java.util.concurrent.TimeUnit.SECONDS)), 2)
        .fieldsGrouping("district-bolt", new Fields("district"));
    builder.setBolt("anomaly-bolt", new AnomalyBolt(), 1).shuffleGrouping("window-bolt");
    builder.setBolt("alert-bolt", new AlertBolt(), 1).shuffleGrouping("anomaly-bolt");

    Config conf = new Config();
    conf.setNumWorkers(1);
    conf.setMessageTimeoutSecs(Math.max(windowSeconds * 2, 600));
    StormSubmitter.submitTopology(name, conf, builder.createTopology());
  }
}
