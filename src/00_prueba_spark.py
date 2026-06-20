from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Prueba Spark - Proyecto FIRMS") \
    .master("local[*]") \
    .getOrCreate()

datos = [
    ("Mexico", 2023, 150),
    ("Mexico", 2024, 210),
    ("Mexico", 2025, 180),
]

columnas = ["pais", "anio", "incendios"]

df = spark.createDataFrame(datos, columnas)

print("=== DATAFRAME DE PRUEBA ===")
df.show()

print("=== CONTEO TOTAL ===")
print(df.count())

spark.stop()
