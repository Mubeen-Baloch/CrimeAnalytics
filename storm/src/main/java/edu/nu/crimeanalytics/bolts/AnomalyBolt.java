package edu.nu.crimeanalytics.bolts;

import java.util.Map;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class AnomalyBolt extends BaseRichBolt {
  private transient OutputCollector collector;
  private long threshold;

  @Override
  public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
    this.collector = collector;
    this.threshold = Long.parseLong(System.getenv().getOrDefault("ANOMALY_THRESHOLD", "50"));
  }

  @Override
  public void execute(Tuple tuple) {
    long count = tuple.getLongByField("event_count");
    if (count > threshold) {
      String severity = count >= threshold * 2 ? "HIGH" : "MEDIUM";
      collector.emit(tuple, new Values(
          tuple.getStringByField("district"),
          tuple.getStringByField("window_timestamp"),
          count,
          threshold,
          severity));
    }
    collector.ack(tuple);
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("district", "alert_timestamp", "event_count", "threshold", "severity"));
  }
}
