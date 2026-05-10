COMPOSE=docker compose -f docker/docker-compose.yml

.PHONY: up down ps logs spark-batch storm-build storm-submit producer dashboard smoke

up:
	$(COMPOSE) up -d

down:
	$(COMPOSE) down

ps:
	$(COMPOSE) ps

logs:
	$(COMPOSE) logs -f

spark-batch:
	$(COMPOSE) exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.jars.ivy=/tmp/.ivy2 --packages org.postgresql:postgresql:42.7.4 /app/spark/batch_job.py --config /app/config/config.yaml

storm-build:
	docker run --rm -v "$$(pwd)/storm:/workspace" -w /workspace maven:3.9-eclipse-temurin-17 mvn -q package

storm-submit: storm-build
	$(COMPOSE) exec storm-nimbus storm jar /storm-app/target/crime-storm-topology-1.0.0.jar edu.nu.crimeanalytics.topology.CrimeAnalyticsTopology

producer:
	python kafka/producer.py --config config/config.yaml

dashboard:
	streamlit run dashboard/app.py

smoke:
	$(COMPOSE) exec postgres pg_isready -U crime_user -d crime_analytics
