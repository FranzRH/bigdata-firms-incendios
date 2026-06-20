import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, year, month, dayofmonth, substring, when, lpad

base_dir = os.path.expanduser("~/bigdata-firms")

ruta_csv = "file://" + os.path.join(base_dir, "data/raw/fire_archive_SV-C2_764880.csv")
ruta_salida = "file://" + os.path.join(base_dir, "data/processed/firms_mexico_limpio.parquet")

spark = SparkSession.builder \
    .appName("Procesamiento NASA FIRMS") \
    .master("local[*]") \
    .config("spark.hadoop.fs.defaultFS", "file:///") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print("=== CARGANDO DATASET NASA FIRMS ===")
print("Ruta CSV:", ruta_csv)

df = spark.read.csv(
    ruta_csv,
    header=True,
    inferSchema=True
)

print("=== ESQUEMA ORIGINAL ===")
df.printSchema()

print("=== PRIMEROS REGISTROS ===")
df.show(5, truncate=False)

total_original = df.count()
print(f"Registros originales: {total_original}")

print("=== LIMPIEZA Y TRANSFORMACIÓN ===")

df_limpio = df \
    .withColumn("latitude", col("latitude").cast("double")) \
    .withColumn("longitude", col("longitude").cast("double")) \
    .withColumn("brightness", col("brightness").cast("double")) \
    .withColumn("scan", col("scan").cast("double")) \
    .withColumn("track", col("track").cast("double")) \
    .withColumn("bright_t31", col("bright_t31").cast("double")) \
    .withColumn("frp", col("frp").cast("double")) \
    .withColumn("acq_date", to_date(col("acq_date"), "yyyy-MM-dd")) \
    .withColumn("acq_time_str", lpad(col("acq_time").cast("string"), 4, "0")) \
    .withColumn("anio", year(col("acq_date"))) \
    .withColumn("mes", month(col("acq_date"))) \
    .withColumn("dia", dayofmonth(col("acq_date"))) \
    .withColumn("hora", substring(col("acq_time_str"), 1, 2).cast("int")) \
    .withColumn(
        "confidence_num",
        when(col("confidence") == "l", 1)
        .when(col("confidence") == "n", 2)
        .when(col("confidence") == "h", 3)
        .otherwise(0)
    ) \
    .withColumn(
        "daynight_num",
        when(col("daynight") == "D", 1)
        .when(col("daynight") == "N", 0)
        .otherwise(None)
    ) \
    .filter(col("latitude").isNotNull()) \
    .filter(col("longitude").isNotNull()) \
    .filter(col("brightness").isNotNull()) \
    .filter(col("frp").isNotNull()) \
    .filter((col("latitude") >= 14) & (col("latitude") <= 33)) \
    .filter((col("longitude") >= -119) & (col("longitude") <= -86)) \
    .filter(col("frp") >= 0)

total_limpio = df_limpio.count()

print("=== RESULTADO DE LIMPIEZA ===")
print(f"Registros originales: {total_original}")
print(f"Registros limpios: {total_limpio}")
print(f"Registros eliminados: {total_original - total_limpio}")

print("=== VALIDACIÓN DE HORA ===")
df_limpio.select("acq_time", "acq_time_str", "hora").show(10, truncate=False)

print("=== MUESTRA LIMPIA ===")
df_limpio.select(
    "latitude", "longitude", "brightness", "frp",
    "confidence", "confidence_num", "daynight", "daynight_num",
    "acq_date", "anio", "mes", "dia", "hora"
).show(10, truncate=False)

print("=== GUARDANDO EN PARQUET ===")
df_limpio.write.mode("overwrite").parquet(ruta_salida)

print(f"Dataset limpio guardado en: {ruta_salida}")

spark.stop()
