package edu.nu.crimeanalytics.bolts;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import java.time.Instant;
import java.util.Map;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.bson.Document;

public class RawEventBolt extends BaseRichBolt {
  private transient OutputCollector collector;
  private transient MongoClient mongoClient;
  private transient MongoCollection<Document> rawEvents;

  @Override
  public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
    this.collector = collector;
    mongoClient = MongoClients.create(System.getenv().getOrDefault("MONGO_URI", "mongodb://mongo:27017"));
    MongoDatabase db = mongoClient.getDatabase(System.getenv().getOrDefault("MONGO_DATABASE", "crime_analytics"));
    rawEvents = db.getCollection("raw_events");
  }

  @Override
  public void execute(Tuple tuple) {
    try {
      rawEvents.insertOne(new Document("case_number", tuple.getStringByField("case_number"))
          .append("event_timestamp", tuple.getStringByField("date"))
          .append("district", tuple.getStringByField("district"))
          .append("primary_type", tuple.getStringByField("primary_type"))
          .append("ingested_at", Instant.now().toString())
          .append("payload", Document.parse(tuple.getStringByField("raw_json"))));
      collector.ack(tuple);
    } catch (Exception ex) {
      collector.reportError(ex);
      collector.fail(tuple);
    }
  }

  @Override
  public void cleanup() {
    if (mongoClient != null) {
      mongoClient.close();
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
  }
}
