import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, avg, max, min, round, col, desc, concat_ws

base_dir = os.path.expanduser("~/bigdata-firms")
ruta_parquet = "file://" + os.path.join(base_dir, "data/processed/firms_mexico_limpio.parquet")
ruta_output = os.path.join(base_dir, "data/output")

spark = SparkSession.builder \
    .appName("Analisis Exploratorio NASA FIRMS") \
    .master("local[*]") \
    .config("spark.hadoop.fs.defaultFS", "file:///") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print("=== CARGANDO PARQUET LIMPIO ===")
df = spark.read.parquet(ruta_parquet)

print("=== TOTAL DE REGISTROS ===")
print(df.count())

df.createOrReplaceTempView("firms")

print("=== RESUMEN GENERAL ===")
resumen_general = spark.sql("""
SELECT
    COUNT(*) AS total_incendios,
    MIN(acq_date) AS fecha_inicio,
    MAX(acq_date) AS fecha_fin,
    ROUND(AVG(brightness), 2) AS brightness_promedio,
    ROUND(AVG(frp), 2) AS frp_promedio,
    ROUND(MAX(frp), 2) AS frp_maximo
FROM firms
""")

resumen_general.show(truncate=False)

print("=== INCENDIOS POR AÑO ===")
incendios_por_anio = spark.sql("""
SELECT
    anio,
    COUNT(*) AS total_incendios,
    ROUND(AVG(brightness), 2) AS brightness_promedio,
    ROUND(AVG(frp), 2) AS frp_promedio
FROM firms
GROUP BY anio
ORDER BY anio
""")

incendios_por_anio.show(50, truncate=False)

print("=== INCENDIOS POR MES ===")
incendios_por_mes = spark.sql("""
SELECT
    anio,
    mes,
    COUNT(*) AS total_incendios,
    ROUND(AVG(brightness), 2) AS brightness_promedio,
    ROUND(AVG(frp), 2) AS frp_promedio
FROM firms
GROUP BY anio, mes
ORDER BY anio, mes
""")

incendios_por_mes.show(100, truncate=False)

print("=== INCENDIOS DÍA / NOCHE ===")
incendios_por_dia_noche = spark.sql("""
SELECT
    daynight,
    CASE
        WHEN daynight = 'D' THEN 'Dia'
        WHEN daynight = 'N' THEN 'Noche'
        ELSE 'Desconocido'
    END AS periodo,
    COUNT(*) AS total_incendios,
    ROUND(AVG(brightness), 2) AS brightness_promedio,
    ROUND(AVG(frp), 2) AS frp_promedio
FROM firms
GROUP BY daynight
ORDER BY total_incendios DESC
""")

incendios_por_dia_noche.show(truncate=False)

print("=== INCENDIOS POR CONFIANZA ===")
incendios_por_confianza = spark.sql("""
SELECT
    confidence,
    CASE
        WHEN confidence = 'l' THEN 'Baja'
        WHEN confidence = 'n' THEN 'Normal'
        WHEN confidence = 'h' THEN 'Alta'
        ELSE 'Desconocida'
    END AS confianza,
    COUNT(*) AS total_incendios,
    ROUND(AVG(brightness), 2) AS brightness_promedio,
    ROUND(AVG(frp), 2) AS frp_promedio
FROM firms
GROUP BY confidence
ORDER BY total_incendios DESC
""")

incendios_por_confianza.show(truncate=False)

print("=== TOP 10 DÍAS CON MÁS INCENDIOS ===")
top_dias_incendios = spark.sql("""
SELECT
    acq_date,
    COUNT(*) AS total_incendios,
    ROUND(AVG(brightness), 2) AS brightness_promedio,
    ROUND(AVG(frp), 2) AS frp_promedio
FROM firms
GROUP BY acq_date
ORDER BY total_incendios DESC
LIMIT 10
""")

top_dias_incendios.show(10, truncate=False)

print("=== ZONAS APROXIMADAS POR CUADRÍCULA ===")
zonas_aproximadas = spark.sql("""
SELECT
    ROUND(latitude, 1) AS lat_zona,
    ROUND(longitude, 1) AS lon_zona,
    COUNT(*) AS total_incendios,
    ROUND(AVG(brightness), 2) AS brightness_promedio,
    ROUND(AVG(frp), 2) AS frp_promedio,
    ROUND(MAX(frp), 2) AS frp_maximo
FROM firms
GROUP BY ROUND(latitude, 1), ROUND(longitude, 1)
ORDER BY total_incendios DESC
LIMIT 30
""")

zonas_aproximadas.show(30, truncate=False)

def guardar_csv_unico(df_spark, nombre):
    salida = "file://" + os.path.join(ruta_output, nombre)
    df_spark.coalesce(1).write.mode("overwrite").option("header", True).csv(salida)
    print(f"Archivo exportado: data/output/{nombre}")

print("=== EXPORTANDO RESULTADOS A CSV ===")
guardar_csv_unico(resumen_general, "resumen_general.csv")
guardar_csv_unico(incendios_por_anio, "incendios_por_anio.csv")
guardar_csv_unico(incendios_por_mes, "incendios_por_mes.csv")
guardar_csv_unico(incendios_por_dia_noche, "incendios_por_dia_noche.csv")
guardar_csv_unico(incendios_por_confianza, "incendios_por_confianza.csv")
guardar_csv_unico(top_dias_incendios, "top_dias_incendios.csv")
guardar_csv_unico(zonas_aproximadas, "zonas_aproximadas.csv")

print("=== ANÁLISIS EXPLORATORIO TERMINADO ===")

spark.stop()
