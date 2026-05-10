package edu.nu.crimeanalytics.bolts;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class ParseBolt extends BaseRichBolt {
  private transient OutputCollector collector;
  private transient ObjectMapper mapper;

  @Override
  public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
    this.collector = collector;
    this.mapper = new ObjectMapper();
  }

  @Override
  public void execute(Tuple tuple) {
    String raw = tuple.getStringByField("value");
    try {
      Map<String, Object> event = mapper.readValue(raw, new TypeReference<Map<String, Object>>() {});
      String[] required = {"case_number", "date", "block", "primary_type", "district", "arrest", "latitude", "longitude"};
      for (String field : required) {
        if (!event.containsKey(field) || event.get(field) == null || event.get(field).toString().isBlank()) {
          throw new IllegalArgumentException("missing " + field);
        }
      }
      String district = normalizeDistrict(event.get("district").toString());
      collector.emit(tuple, new Values(
          event.get("case_number").toString(),
          event.get("date").toString(),
          event.get("block").toString(),
          event.get("primary_type").toString(),
          district,
          Boolean.parseBoolean(event.get("arrest").toString()),
          Double.parseDouble(event.get("latitude").toString()),
          Double.parseDouble(event.get("longitude").toString()),
          raw));
      collector.ack(tuple);
    } catch (Exception ex) {
      System.err.println("Discarding malformed Kafka message: " + ex.getMessage() + " raw=" + raw);
      collector.ack(tuple);
    }
  }

  private String normalizeDistrict(String district) {
    String digits = district.replaceAll("\\D", "");
    if (digits.isEmpty()) {
      return "000";
    }
    return String.format("%03d", Integer.parseInt(digits));
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("case_number", "date", "block", "primary_type", "district", "arrest", "latitude", "longitude", "raw_json"));
  }
}
