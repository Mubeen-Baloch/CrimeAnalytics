# Real-Time Crime Analytics & Intelligent Alert System

Course project for CS4109 Fundamentals of Big Data Analytics. The system uses a Lambda Architecture with Spark batch analytics, Kafka + Storm streaming anomaly detection, PostgreSQL/MongoDB serving storage, and a Streamlit dashboard.

## Project Layout

- `docker/docker-compose.yml` starts Kafka, Zookeeper, Spark, Storm, PostgreSQL, MongoDB, and Streamlit.
- `config/config.yaml` contains dataset paths, Kafka topic/rate, Storm window settings, and database credentials.
- `kafka/producer.py` replays the Crime CSV to Kafka as JSON events.
- `storm/` contains the Java Storm topology and bolts.
- `spark/batch_job.py` runs the required historical analytics and K-Means hotspot detection.
- `dashboard/app.py` visualizes alerts, trends, arrest rates, and hotspot centroids.
- `db/` initializes PostgreSQL tables and MongoDB collections.
- `data/` holds the Chicago Open Data CSV files.

## Local Python Environment

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

## Start Infrastructure

```powershell
docker compose -f docker/docker-compose.yml up -d
```

Useful URLs:

- Spark UI: <http://localhost:8080>
- Storm UI: <http://localhost:8081>
- Streamlit dashboard: <http://localhost:8501>
- MongoDB host port: `localhost:27018` mapped to container `mongo:27017`

## Run the Pipeline

Run Spark batch analytics:

```powershell
docker compose -f docker/docker-compose.yml exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.jars.ivy=/tmp/.ivy2 --packages org.postgresql:postgresql:42.7.4 /app/spark/batch_job.py --config /app/config/config.yaml
```

Build and submit Storm topology:

```powershell
docker run --rm -v ${PWD}/storm:/workspace -w /workspace maven:3.9-eclipse-temurin-17 mvn package
docker compose -f docker/docker-compose.yml exec storm-nimbus storm jar /storm-app/target/crime-storm-topology-1.0.0.jar edu.nu.crimeanalytics.topology.CrimeAnalyticsTopology
```

Start the Kafka producer from the venv:

```powershell
python kafka/producer.py --config config/config.yaml
```

## Demo Checklist

1. Show `docker compose ps` with all infrastructure running.
2. Open Storm UI and show the submitted `crime-analytics` topology.
3. Run the producer and show Kafka/Storm activity.
4. Run the Spark job and show PostgreSQL tables populated.
5. Lower `ANOMALY_THRESHOLD` in `docker/docker-compose.yml` temporarily for a quick alert demo if needed.
6. Open the Streamlit dashboard and show alerts, trends, arrest rates, and hotspot map.

## Notes

- Full datasets are used by default. For faster rehearsals, set `producer_max_rows` in `config/config.yaml` or point the glob values to smaller sampled CSVs.
- Spark uses explicit schemas; schema inference is not used.
- The Sex Offenders dataset does not include a district field. The implementation assigns records deterministically across police districts for density reporting and documents this as a source-schema limitation for the report.
