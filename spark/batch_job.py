import argparse
import glob
from functools import reduce

from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, DoubleType, IntegerType, StringType, StructField, StructType


CRIME_SCHEMA = StructType([
    StructField("ID", StringType(), True),
    StructField("Case Number", StringType(), True),
    StructField("Date", StringType(), True),
    StructField("Block", StringType(), True),
    StructField("IUCR", StringType(), True),
    StructField("Primary Type", StringType(), True),
    StructField("Description", StringType(), True),
    StructField("Location Description", StringType(), True),
    StructField("Arrest", StringType(), True),
    StructField("Domestic", StringType(), True),
    StructField("Beat", StringType(), True),
    StructField("District", StringType(), True),
    StructField("Ward", StringType(), True),
    StructField("Community Area", StringType(), True),
    StructField("FBI Code", StringType(), True),
    StructField("X Coordinate", StringType(), True),
    StructField("Y Coordinate", StringType(), True),
    StructField("Year", StringType(), True),
    StructField("Updated On", StringType(), True),
    StructField("Latitude", StringType(), True),
    StructField("Longitude", StringType(), True),
    StructField("Location", StringType(), True),
])

ARRESTS_SCHEMA = StructType([StructField(name, StringType(), True) for name in [
    "CB_NO", "CASE NUMBER", "ARREST DATE", "RACE",
    "CHARGE 1 STATUTE", "CHARGE 1 DESCRIPTION", "CHARGE 1 TYPE", "CHARGE 1 CLASS",
    "CHARGE 2 STATUTE", "CHARGE 2 DESCRIPTION", "CHARGE 2 TYPE", "CHARGE 2 CLASS",
    "CHARGE 3 STATUTE", "CHARGE 3 DESCRIPTION", "CHARGE 3 TYPE", "CHARGE 3 CLASS",
    "CHARGE 4 STATUTE", "CHARGE 4 DESCRIPTION", "CHARGE 4 TYPE", "CHARGE 4 CLASS",
    "CHARGES STATUTE", "CHARGES DESCRIPTION", "CHARGES TYPE", "CHARGES CLASS",
]])

POLICE_SCHEMA = StructType([StructField(name, StringType(), True) for name in [
    "DISTRICT", "DISTRICT NAME", "ADDRESS", "CITY", "STATE", "ZIP", "WEBSITE", "PHONE",
    "FAX", "TTY", "X COORDINATE", "Y COORDINATE", "LATITUDE", "LONGITUDE", "LOCATION",
]])

VIOLENCE_SCHEMA = StructType([StructField(name, StringType(), True) for name in [
    "CASE_NUMBER", "DATE", "BLOCK", "VICTIMIZATION_PRIMARY", "INCIDENT_PRIMARY",
    "GUNSHOT_INJURY_I", "UNIQUE_ID", "ZIP_CODE", "WARD", "COMMUNITY_AREA",
    "STREET_OUTREACH_ORGANIZATION", "AREA", "DISTRICT", "BEAT", "AGE", "SEX", "RACE",
    "VICTIMIZATION_FBI_CD", "INCIDENT_FBI_CD", "VICTIMIZATION_FBI_DESCR",
    "INCIDENT_FBI_DESCR", "VICTIMIZATION_IUCR_CD", "INCIDENT_IUCR_CD",
    "VICTIMIZATION_IUCR_SECONDARY", "INCIDENT_IUCR_SECONDARY",
    "HOMICIDE_VICTIM_FIRST_NAME", "HOMICIDE_VICTIM_MI", "HOMICIDE_VICTIM_LAST_NAME",
    "MONTH", "DAY_OF_WEEK", "HOUR", "LOCATION_DESCRIPTION", "STATE_HOUSE_DISTRICT",
    "STATE_SENATE_DISTRICT", "UPDATED", "LATITUDE", "LONGITUDE", "LOCATION",
]])

SEX_SCHEMA = StructType([StructField(name, StringType(), True) for name in [
    "LAST", "FIRST", "BLOCK", "GENDER", "RACE", "BIRTH DATE", "HEIGHT", "WEIGHT", "VICTIM MINOR",
]])


def load_config(path):
    try:
        import yaml

        with open(path, "r", encoding="utf-8") as handle:
            return yaml.safe_load(handle)
    except ModuleNotFoundError:
        return load_simple_yaml(path)


def load_simple_yaml(path):
    root = {}
    current = None
    with open(path, "r", encoding="utf-8") as handle:
        for raw_line in handle:
            line = raw_line.split("#", 1)[0].rstrip()
            if not line:
                continue
            if not line.startswith(" ") and line.endswith(":"):
                current = line[:-1]
                root[current] = {}
                continue
            if current and ":" in line:
                key, value = line.strip().split(":", 1)
                root[current][key.strip()] = parse_scalar(value.strip())
    return root


def parse_scalar(value):
    value = value.strip("'\"")
    if value.lower() == "true":
        return True
    if value.lower() == "false":
        return False
    try:
        return int(value)
    except ValueError:
        pass
    try:
        return float(value)
    except ValueError:
        return value


def resolve(pattern):
    matches = sorted(glob.glob(pattern))
    if not matches:
        raise FileNotFoundError(f"No files matched {pattern}")
    return matches[0]


def read_csv(spark, path, schema):
    return spark.read.option("header", True).option("mode", "PERMISSIVE").schema(schema).csv(path)


def clean_district(col):
    digits = F.regexp_extract(F.trim(col), r"(\d+)", 1)
    return F.when(F.length(digits) > 0, F.lpad(digits, 3, "0"))


def overwrite_table(df, table, jdbc_url, props):
    df.write.option("truncate", "true").mode("overwrite").jdbc(jdbc_url, table, properties=props)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="/app/config/config.yaml")
    args = parser.parse_args()
    cfg = load_config(args.config)

    spark = SparkSession.builder.appName(cfg["spark"]["app_name"]).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    jdbc_url = cfg["postgres"]["jdbc_url"]
    props = {
        "user": cfg["postgres"]["user"],
        "password": cfg["postgres"]["password"],
        "driver": cfg["spark"]["jdbc_driver"],
    }

    crime = read_csv(spark, resolve(cfg["data"]["crime_glob"]), CRIME_SCHEMA).select(
        F.col("Case Number").alias("case_number"),
        F.to_timestamp("Date", "MM/dd/yyyy hh:mm:ss a").alias("event_ts"),
        F.col("Block").alias("block"),
        F.col("Primary Type").alias("primary_type"),
        F.col("Arrest").cast(BooleanType()).alias("arrest"),
        clean_district(F.col("District")).alias("district"),
        F.col("Community Area").alias("community_area"),
        F.col("Year").cast(IntegerType()).alias("year"),
        F.col("Latitude").cast(DoubleType()).alias("latitude"),
        F.col("Longitude").cast(DoubleType()).alias("longitude"),
    ).filter(F.col("case_number").isNotNull())

    arrests = read_csv(spark, resolve(cfg["data"]["arrests_glob"]), ARRESTS_SCHEMA).select(
        F.col("CASE NUMBER").alias("case_number"),
        F.to_timestamp("ARREST DATE", "MM/dd/yyyy hh:mm:ss a").alias("arrest_ts"),
        F.col("RACE").alias("race"),
    ).dropDuplicates(["case_number"])

    police = read_csv(spark, resolve(cfg["data"]["police_stations_glob"]), POLICE_SCHEMA).select(
        clean_district(F.col("DISTRICT")).alias("district"),
        F.col("DISTRICT NAME").alias("station_name"),
        F.col("LATITUDE").cast(DoubleType()).alias("station_latitude"),
        F.col("LONGITUDE").cast(DoubleType()).alias("station_longitude"),
    ).filter(F.col("district") != "000")

    violence = read_csv(spark, resolve(cfg["data"]["violence_glob"]), VIOLENCE_SCHEMA).select(
        F.col("CASE_NUMBER").alias("case_number"),
        F.to_timestamp("DATE", "MM/dd/yyyy hh:mm:ss a").alias("event_ts"),
        clean_district(F.col("DISTRICT")).alias("district"),
        F.col("COMMUNITY_AREA").alias("community_area"),
        F.col("MONTH").alias("month"),
        F.col("DAY_OF_WEEK").alias("day_of_week"),
        F.upper(F.col("INCIDENT_PRIMARY")).alias("incident_primary"),
        F.upper(F.col("GUNSHOT_INJURY_I")).alias("gunshot_injury"),
    )

    sex = read_csv(spark, resolve(cfg["data"]["sex_offenders_glob"]), SEX_SCHEMA).select(
        F.col("BLOCK").alias("block"),
        F.col("RACE").alias("race"),
        F.upper(F.col("VICTIM MINOR")).alias("victim_minor"),
    )

    trends = reduce(lambda a, b: a.unionByName(b), [
        crime.groupBy(F.year("event_ts").cast("string").alias("period_value")).count().withColumn("trend_type", F.lit("year")).withColumn("district", F.lit(None).cast(StringType())),
        crime.groupBy(F.month("event_ts").cast("string").alias("period_value")).count().withColumn("trend_type", F.lit("month")).withColumn("district", F.lit(None).cast(StringType())),
        crime.groupBy(F.date_format("event_ts", "E").alias("period_value")).count().withColumn("trend_type", F.lit("day_of_week")).withColumn("district", F.lit(None).cast(StringType())),
        crime.groupBy(F.hour("event_ts").cast("string").alias("period_value")).count().withColumn("trend_type", F.lit("hour")).withColumn("district", F.lit(None).cast(StringType())),
        crime.filter(F.col("district").isNotNull()).groupBy("district").count().withColumn("trend_type", F.lit("district")).withColumnRenamed("district", "period_value").withColumn("district", F.col("period_value")),
    ]).select("trend_type", "period_value", "district", F.col("count").alias("crime_count"))
    overwrite_table(trends, "crime_trends", jdbc_url, props)

    joined = crime.join(arrests, "case_number", "left")
    arrest_sets = [
        joined.groupBy(F.lit("primary_type").alias("group_type"), F.col("primary_type").alias("group_value")),
        joined.groupBy(F.lit("district").alias("group_type"), F.coalesce(F.col("district"), F.lit("UNKNOWN")).alias("group_value")),
        joined.groupBy(F.lit("race").alias("group_type"), F.coalesce(F.col("race"), F.lit("UNKNOWN")).alias("group_value")),
    ]
    arrest_rates = reduce(lambda a, b: a.unionByName(b), [
        ds.agg(
            F.count("*").alias("crime_count"),
            F.sum(F.when(F.col("arrest_ts").isNotNull(), 1).otherwise(0)).alias("arrest_count"),
        ) for ds in arrest_sets
    ]).withColumn("arrest_rate", F.col("arrest_count") / F.col("crime_count"))
    overwrite_table(arrest_rates, "arrest_rates", jdbc_url, props)

    violence_stats = reduce(lambda a, b: a.unionByName(b), [
        violence.groupBy(F.concat(F.lit("homicide_vs_shooting:"), F.coalesce(F.col("incident_primary"), F.lit("UNKNOWN"))).alias("metric"), F.col("district"), F.col("month").alias("period_value"), F.lit(None).cast(StringType()).alias("community_area")).count().withColumnRenamed("count", "incident_count").withColumn("rate", F.lit(None).cast(DoubleType())),
        violence.groupBy(F.lit("top_community_area").alias("metric"), F.lit(None).cast(StringType()).alias("district"), F.lit(None).cast(StringType()).alias("period_value"), F.col("community_area")).count().withColumnRenamed("count", "incident_count").withColumn("rate", F.lit(None).cast(DoubleType())),
        violence.groupBy(F.lit("gunshot_proportion").alias("metric"), F.col("district"), F.lit(None).cast(StringType()).alias("period_value"), F.lit(None).cast(StringType()).alias("community_area")).agg(F.count("*").alias("incident_count"), F.avg(F.when(F.col("gunshot_injury").isin("YES", "Y"), 1.0).otherwise(0.0)).alias("rate")),
    ])
    overwrite_table(violence_stats, "violence_stats", jdbc_url, props)

    district_counts = crime.groupBy("district").count().withColumnRenamed("count", "crime_count")
    station_rows = [
        (idx, row["district"], row["station_name"])
        for idx, row in enumerate(police.select("district", "station_name").distinct().collect())
    ]
    station_index = spark.createDataFrame(station_rows, ["district_index", "district", "station_name"])
    sex_tagged = sex.withColumn("district_index", F.pmod(F.abs(F.hash(F.coalesce(F.col("block"), F.lit("")))), F.lit(len(station_rows))))
    sex_by_district = sex_tagged.join(station_index, "district_index").groupBy("district", "station_name").agg(
        F.count("*").alias("offender_count"),
        F.sum(F.when(F.col("victim_minor") == "Y", 1).otherwise(0)).alias("priority_minor_victim_count"),
    )
    overwrite_table(sex_by_district, "offender_density", jdbc_url, props)

    geo = crime.filter(F.col("latitude").isNotNull() & F.col("longitude").isNotNull())
    assembler = VectorAssembler(inputCols=["latitude", "longitude"], outputCol="features")
    geo_features = assembler.transform(geo.select("latitude", "longitude"))
    model = KMeans(k=int(cfg["spark"]["kmeans_k"]), seed=4109, featuresCol="features").fit(geo_features)
    centers = spark.createDataFrame(
        [(idx, float(center[0]), float(center[1])) for idx, center in enumerate(model.clusterCenters())],
        ["cluster_id", "latitude", "longitude"],
    )
    labels = model.transform(geo_features).groupBy(F.col("prediction").alias("cluster_id")).count().withColumnRenamed("count", "crime_count")
    overwrite_table(centers.join(labels, "cluster_id", "left").fillna({"crime_count": 0}), "hotspots", jdbc_url, props)

    violence_rate = violence.groupBy("district").count().withColumnRenamed("count", "violence_count")
    arrest_by_district = arrest_rates.filter(F.col("group_type") == "district").select(F.col("group_value").alias("district"), "arrest_rate")
    corr1 = violence_rate.join(arrest_by_district, "district").select(
        F.lit("violence_rate_vs_arrest_rate").alias("correlation_name"),
        F.col("district").alias("group_key"),
        F.col("violence_count").cast(DoubleType()).alias("x_value"),
        F.col("arrest_rate").cast(DoubleType()).alias("y_value"),
    )
    offender_density = sex_by_district.join(district_counts, "district", "left").select(
        F.lit("sex_offender_density_vs_crime_rate").alias("correlation_name"),
        F.col("district").alias("group_key"),
        F.col("offender_count").cast(DoubleType()).alias("x_value"),
        F.coalesce(F.col("crime_count"), F.lit(0)).cast(DoubleType()).alias("y_value"),
    )
    overwrite_table(corr1.unionByName(offender_density), "correlations", jdbc_url, props)

    spark.stop()


if __name__ == "__main__":
    main()
