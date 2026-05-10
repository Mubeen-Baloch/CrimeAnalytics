db = db.getSiblingDB("crime_analytics");
db.createCollection("raw_events");
db.createCollection("alert_logs");
db.raw_events.createIndex({ "case_number": 1 });
db.raw_events.createIndex({ "district": 1, "event_timestamp": -1 });
db.alert_logs.createIndex({ "district": 1, "alert_timestamp": -1 });
