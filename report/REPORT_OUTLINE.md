# Technical Report Outline

## 1. Introduction
- Problem context and motivation.
- Lambda Architecture overview.

## 2. Dataset Description
- Crime, Police Stations, Arrests, Violence Reduction, Sex Offenders.
- Schema inconsistencies and cleaning decisions.

## 3. System Design
- Batch layer: Spark preprocessing, analytics, ML.
- Speed layer: Kafka producer and Storm topology.
- Serving layer: PostgreSQL and MongoDB.
- Dashboard layer: Streamlit visualizations.

## 4. Methodology
- Explicit Spark schemas.
- Null handling and type casting.
- Sliding-window anomaly detection.
- K-Means geospatial hotspot detection.
- Cross-dataset joins and correlations.

## 5. Results
- Crime trends.
- Arrest rates.
- Violence and gunshot analysis.
- Offender density limitation and deterministic district assignment.
- Hotspot centroids.
- Alert examples.

## 6. Challenges
- Large local CSV files.
- Dirty schema names and missing join keys.
- Container orchestration.

## 7. AI Use Disclosure
- AI assistance was used to scaffold and implement the project code and documentation.
- Include screenshots of prompts if required by the course team.
