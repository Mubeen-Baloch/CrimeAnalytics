package edu.nu.crimeanalytics.bolts;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;

public class WindowCountBolt extends BaseWindowedBolt {
  private transient OutputCollector collector;

  @Override
  public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
    this.collector = collector;
  }

  @Override
  public void execute(TupleWindow inputWindow) {
    Map<String, Long> counts = new HashMap<>();
    for (Tuple tuple : inputWindow.get()) {
      String district = tuple.getStringByField("district");
      counts.put(district, counts.getOrDefault(district, 0L) + 1L);
    }
    String timestamp = Instant.now().toString();
    counts.forEach((district, count) -> collector.emit(new Values(district, count, timestamp)));
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("district", "event_count", "window_timestamp"));
  }
}
