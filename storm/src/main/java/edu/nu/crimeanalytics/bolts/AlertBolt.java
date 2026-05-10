package edu.nu.crimeanalytics.bolts;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Map;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.bson.Document;

public class AlertBolt extends BaseRichBolt {
  private transient OutputCollector collector;
  private transient Connection pg;
  private transient MongoClient mongoClient;
  private transient MongoCollection<Document> alerts;

  @Override
  public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
    this.collector = collector;
    try {
      pg = DriverManager.getConnection(
          System.getenv().getOrDefault("POSTGRES_JDBC_URL", "jdbc:postgresql://postgres:5432/crime_analytics"),
          System.getenv().getOrDefault("POSTGRES_USER", "crime_user"),
          System.getenv().getOrDefault("POSTGRES_PASSWORD", "crime_pass"));
      mongoClient = MongoClients.create(System.getenv().getOrDefault("MONGO_URI", "mongodb://mongo:27017"));
      MongoDatabase db = mongoClient.getDatabase(System.getenv().getOrDefault("MONGO_DATABASE", "crime_analytics"));
      alerts = db.getCollection("alert_logs");
    } catch (Exception ex) {
      throw new RuntimeException("Unable to initialize alert persistence", ex);
    }
  }

  @Override
  public void execute(Tuple tuple) {
    try {
      String district = tuple.getStringByField("district");
      Instant timestamp = Instant.parse(tuple.getStringByField("alert_timestamp"));
      long eventCount = tuple.getLongByField("event_count");
      long threshold = tuple.getLongByField("threshold");
      String severity = tuple.getStringByField("severity");
      Document doc = new Document("district", district)
          .append("alert_timestamp", timestamp.toString())
          .append("event_count", eventCount)
          .append("threshold", threshold)
          .append("severity", severity);

      alerts.insertOne(doc);
      try (PreparedStatement stmt = pg.prepareStatement(
          "insert into alerts (district, alert_timestamp, event_count, threshold_value, severity, payload) values (?, ?, ?, ?, ?, ?::jsonb)")) {
        stmt.setString(1, district);
        stmt.setTimestamp(2, Timestamp.from(timestamp));
        stmt.setLong(3, eventCount);
        stmt.setLong(4, threshold);
        stmt.setString(5, severity);
        stmt.setString(6, doc.toJson());
        stmt.executeUpdate();
      }
      collector.ack(tuple);
    } catch (Exception ex) {
      collector.reportError(ex);
      collector.fail(tuple);
    }
  }

  @Override
  public void cleanup() {
    try {
      if (pg != null) {
        pg.close();
      }
    } catch (Exception ignored) {
    }
    if (mongoClient != null) {
      mongoClient.close();
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
  }
}
