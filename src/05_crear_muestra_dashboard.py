import os
import pandas as pd

base_dir = os.path.expanduser("~/bigdata-firms")

ruta_entrada = os.path.join(base_dir, "data/output/puntos_cluster_dashboard_final.csv")
ruta_salida = os.path.join(base_dir, "data/output/puntos_cluster_dashboard_sample.csv")

print("=== CARGANDO PUNTOS COMPLETOS ===")
df = pd.read_csv(ruta_entrada)

print("Registros originales:", len(df))

n = min(50000, len(df))

df_sample = df.sample(n=n, random_state=42)

df_sample.to_csv(ruta_salida, index=False)

print("Registros en muestra:", len(df_sample))
print("Archivo generado:", ruta_salida)
