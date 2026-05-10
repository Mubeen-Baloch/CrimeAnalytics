CREATE TABLE IF NOT EXISTS crime_trends (
  id BIGSERIAL PRIMARY KEY,
  trend_type TEXT NOT NULL,
  period_value TEXT NOT NULL,
  district TEXT,
  crime_count BIGINT NOT NULL,
  generated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS arrest_rates (
  id BIGSERIAL PRIMARY KEY,
  group_type TEXT NOT NULL,
  group_value TEXT NOT NULL,
  crime_count BIGINT NOT NULL,
  arrest_count BIGINT NOT NULL,
  arrest_rate DOUBLE PRECISION NOT NULL,
  generated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS violence_stats (
  id BIGSERIAL PRIMARY KEY,
  metric TEXT NOT NULL,
  district TEXT,
  period_value TEXT,
  community_area TEXT,
  incident_count BIGINT NOT NULL,
  rate DOUBLE PRECISION,
  generated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS offender_density (
  id BIGSERIAL PRIMARY KEY,
  district TEXT NOT NULL,
  offender_count BIGINT NOT NULL,
  priority_minor_victim_count BIGINT NOT NULL,
  station_name TEXT,
  generated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS hotspots (
  id BIGSERIAL PRIMARY KEY,
  cluster_id INT NOT NULL,
  latitude DOUBLE PRECISION NOT NULL,
  longitude DOUBLE PRECISION NOT NULL,
  crime_count BIGINT NOT NULL,
  generated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS correlations (
  id BIGSERIAL PRIMARY KEY,
  correlation_name TEXT NOT NULL,
  group_key TEXT NOT NULL,
  x_value DOUBLE PRECISION NOT NULL,
  y_value DOUBLE PRECISION NOT NULL,
  generated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS alerts (
  id BIGSERIAL PRIMARY KEY,
  district TEXT NOT NULL,
  alert_timestamp TIMESTAMPTZ NOT NULL,
  event_count BIGINT NOT NULL,
  threshold_value BIGINT NOT NULL,
  severity TEXT NOT NULL,
  payload JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_alerts_timestamp ON alerts(alert_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_alerts_district ON alerts(district);
CREATE INDEX IF NOT EXISTS idx_trends_type_period ON crime_trends(trend_type, period_value);
