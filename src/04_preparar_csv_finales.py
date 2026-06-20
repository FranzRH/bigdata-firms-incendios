import os
import glob
import shutil

base_dir = os.path.expanduser("~/bigdata-firms")
output_dir = os.path.join(base_dir, "data/output")

carpetas = [
    "resumen_general.csv",
    "incendios_por_anio.csv",
    "incendios_por_mes.csv",
    "incendios_por_dia_noche.csv",
    "incendios_por_confianza.csv",
    "top_dias_incendios.csv",
    "zonas_aproximadas.csv",
    "resumen_clusters.csv",
    "puntos_cluster_dashboard.csv",
    "metricas_modelo.csv"
]

print("=== PREPARANDO CSV FINALES ===")

for carpeta in carpetas:
    ruta_carpeta = os.path.join(output_dir, carpeta)
    nombre_final = carpeta.replace(".csv", "_final.csv")
    ruta_final = os.path.join(output_dir, nombre_final)

    archivos_part = glob.glob(os.path.join(ruta_carpeta, "part-*.csv"))

    if archivos_part:
        shutil.copy(archivos_part[0], ruta_final)
        print(f"Creado: data/output/{nombre_final}")
    else:
        print(f"No se encontró archivo part en: {carpeta}")

print("=== CSV FINALES LISTOS ===")
