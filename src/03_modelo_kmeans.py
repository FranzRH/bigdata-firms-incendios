import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, max, round, when, desc
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator

base_dir = os.path.expanduser("~/bigdata-firms")
ruta_parquet = "file://" + os.path.join(base_dir, "data/processed/firms_mexico_limpio.parquet")
ruta_output = os.path.join(base_dir, "data/output")

spark = SparkSession.builder \
    .appName("Modelo KMeans NASA FIRMS") \
    .master("local[*]") \
    .config("spark.hadoop.fs.defaultFS", "file:///") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print("=== CARGANDO DATASET LIMPIO ===")
df = spark.read.parquet(ruta_parquet)

print("Registros cargados:", df.count())

print("=== SELECCIÓN DE VARIABLES PARA CLUSTERING ===")
columnas_modelo = [
    "latitude",
    "longitude",
    "brightness",
    "frp",
    "confidence_num",
    "daynight_num"
]

df_modelo = df.select(columnas_modelo + [
    "acq_date", "anio", "mes", "dia", "hora",
    "confidence", "daynight"
]).dropna()

print("Registros para modelo:", df_modelo.count())

assembler = VectorAssembler(
    inputCols=columnas_modelo,
    outputCol="features"
)

df_vector = assembler.transform(df_modelo)

scaler = StandardScaler(
    inputCol="features",
    outputCol="scaled_features",
    withStd=True,
    withMean=True
)

scaler_model = scaler.fit(df_vector)
df_scaled = scaler_model.transform(df_vector)

print("=== ENTRENANDO K-MEANS ===")
kmeans = KMeans(
    featuresCol="scaled_features",
    predictionCol="cluster",
    k=3,
    seed=42
)

modelo = kmeans.fit(df_scaled)
df_cluster = modelo.transform(df_scaled)

print("=== EVALUACIÓN DEL CLUSTERING ===")
evaluator = ClusteringEvaluator(
    featuresCol="scaled_features",
    predictionCol="cluster",
    metricName="silhouette",
    distanceMeasure="squaredEuclidean"
)

silhouette = evaluator.evaluate(df_cluster)
print(f"Silhouette Score: {silhouette:.4f}")

print("=== CENTROS DE LOS CLUSTERS ===")
centros = modelo.clusterCenters()
for i, centro in enumerate(centros):
    print(f"Cluster {i}: {centro}")

print("=== RESUMEN POR CLUSTER ===")
resumen_clusters = df_cluster.groupBy("cluster").agg(
    count("*").alias("total_incendios"),
    round(avg("latitude"), 4).alias("lat_promedio"),
    round(avg("longitude"), 4).alias("lon_promedio"),
    round(avg("brightness"), 2).alias("brightness_promedio"),
    round(avg("frp"), 2).alias("frp_promedio"),
    round(max("frp"), 2).alias("frp_maximo"),
    round(avg("confidence_num"), 2).alias("confianza_promedio"),
    round(avg("daynight_num"), 2).alias("proporcion_dia")
).orderBy(desc("frp_promedio"))

resumen_clusters.show(truncate=False)

print("=== ASIGNANDO NIVEL DE RIESGO ===")
clusters_ordenados = resumen_clusters.select("cluster").collect()

riesgo_map = {}
niveles = ["Alto", "Medio", "Bajo"]

for idx, fila in enumerate(clusters_ordenados):
    riesgo_map[fila["cluster"]] = niveles[idx]

print("Mapa de riesgo asignado:", riesgo_map)

df_cluster_riesgo = df_cluster.withColumn(
    "riesgo",
    when(col("cluster") == clusters_ordenados[0]["cluster"], "Alto")
    .when(col("cluster") == clusters_ordenados[1]["cluster"], "Medio")
    .otherwise("Bajo")
)

resumen_clusters_riesgo = df_cluster_riesgo.groupBy("cluster", "riesgo").agg(
    count("*").alias("total_incendios"),
    round(avg("latitude"), 4).alias("lat_promedio"),
    round(avg("longitude"), 4).alias("lon_promedio"),
    round(avg("brightness"), 2).alias("brightness_promedio"),
    round(avg("frp"), 2).alias("frp_promedio"),
    round(max("frp"), 2).alias("frp_maximo"),
    round(avg("confidence_num"), 2).alias("confianza_promedio"),
    round(avg("daynight_num"), 2).alias("proporcion_dia")
).orderBy(desc("frp_promedio"))

print("=== RESUMEN FINAL CON RIESGO ===")
resumen_clusters_riesgo.show(truncate=False)

print("=== MUESTRA DE PUNTOS CON CLUSTER ===")
df_cluster_riesgo.select(
    "latitude", "longitude", "brightness", "frp",
    "confidence", "daynight", "acq_date",
    "anio", "mes", "hora", "cluster", "riesgo"
).show(20, truncate=False)

def guardar_csv_unico(df_spark, nombre):
    salida = "file://" + os.path.join(ruta_output, nombre)
    df_spark.coalesce(1).write.mode("overwrite").option("header", True).csv(salida)
    print(f"Archivo exportado: data/output/{nombre}")

print("=== EXPORTANDO RESULTADOS DEL MODELO ===")

guardar_csv_unico(resumen_clusters_riesgo, "resumen_clusters.csv")

puntos_dashboard = df_cluster_riesgo.select(
    "latitude", "longitude", "brightness", "frp",
    "confidence", "daynight", "acq_date",
    "anio", "mes", "hora", "cluster", "riesgo"
)

guardar_csv_unico(puntos_dashboard, "puntos_cluster_dashboard.csv")

metricas_modelo = spark.createDataFrame(
    [(3, float(silhouette))],
    ["k", "silhouette_score"]
)

guardar_csv_unico(metricas_modelo, "metricas_modelo.csv")

print("=== MODELO K-MEANS TERMINADO ===")

spark.stop()
