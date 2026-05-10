package edu.nu.crimeanalytics.bolts;

import java.util.Map;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class DistrictBolt extends BaseRichBolt {
  private transient OutputCollector collector;

  @Override
  public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
    this.collector = collector;
  }

  @Override
  public void execute(Tuple tuple) {
    collector.emit(tuple, new Values(
        tuple.getStringByField("district"),
        tuple.getStringByField("case_number"),
        tuple.getStringByField("date"),
        tuple.getStringByField("primary_type"),
        tuple.getStringByField("raw_json")));
    collector.ack(tuple);
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("district", "case_number", "date", "primary_type", "raw_json"));
  }
}
