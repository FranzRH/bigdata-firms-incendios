import os
import pandas as pd
import streamlit as st
import plotly.express as px
import pydeck as pdk

st.set_page_config(
    page_title="Dashboard NASA FIRMS - Incendios México",
    layout="wide"
)

base_dir = os.path.expanduser("~/bigdata-firms")
output_dir = os.path.join(base_dir, "data/output")

@st.cache_data
def cargar_csv(nombre):
    return pd.read_csv(os.path.join(output_dir, nombre))

resumen_general = cargar_csv("resumen_general_final.csv")
incendios_anio = cargar_csv("incendios_por_anio_final.csv")
incendios_mes = cargar_csv("incendios_por_mes_final.csv")
dia_noche = cargar_csv("incendios_por_dia_noche_final.csv")
confianza = cargar_csv("incendios_por_confianza_final.csv")
top_dias = cargar_csv("top_dias_incendios_final.csv")
zonas = cargar_csv("zonas_aproximadas_final.csv")
clusters = cargar_csv("resumen_clusters_final.csv")
metricas = cargar_csv("metricas_modelo_final.csv")
puntos = cargar_csv("puntos_cluster_dashboard_sample.csv")

st.title("Pipeline Big Data para análisis de incendios forestales en México")
st.caption("Dataset: NASA FIRMS VIIRS S-NPP | Procesamiento: PySpark | Modelo: K-Means con SparkML")

total_incendios = int(resumen_general.loc[0, "total_incendios"])
fecha_inicio = resumen_general.loc[0, "fecha_inicio"]
fecha_fin = resumen_general.loc[0, "fecha_fin"]
frp_promedio = float(resumen_general.loc[0, "frp_promedio"])
frp_maximo = float(resumen_general.loc[0, "frp_maximo"])
silhouette = float(metricas.loc[0, "silhouette_score"])

col1, col2, col3, col4, col5 = st.columns(5)

col1.metric("Total de detecciones", f"{total_incendios:,}")
col2.metric("Fecha inicial", str(fecha_inicio))
col3.metric("Fecha final", str(fecha_fin))
col4.metric("FRP promedio", f"{frp_promedio:.2f}")
col5.metric("Silhouette", f"{silhouette:.4f}")

st.divider()

st.subheader("Análisis temporal")

col_a, col_b = st.columns(2)

with col_a:
    fig_anio = px.bar(
        incendios_anio,
        x="anio",
        y="total_incendios",
        title="Detecciones de incendios por año",
        text="total_incendios"
    )
    st.plotly_chart(fig_anio, width="stretch")

with col_b:
    fig_mes = px.line(
        incendios_mes,
        x="mes",
        y="total_incendios",
        color="anio",
        markers=True,
        title="Detecciones de incendios por mes"
    )
    st.plotly_chart(fig_mes, width="stretch")

st.subheader("Distribución por periodo y confianza")

col_c, col_d = st.columns(2)

with col_c:
    fig_dia_noche = px.pie(
        dia_noche,
        names="periodo",
        values="total_incendios",
        title="Detecciones de día y noche"
    )
    st.plotly_chart(fig_dia_noche, width="stretch")

with col_d:
    fig_confianza = px.bar(
        confianza,
        x="confianza",
        y="total_incendios",
        title="Detecciones por nivel de confianza",
        text="total_incendios"
    )
    st.plotly_chart(fig_confianza, width="stretch")

st.divider()

st.subheader("Modelo K-Means y niveles de riesgo")

st.write(
    "El modelo agrupó las detecciones en 3 clusters usando latitud, longitud, brillo, FRP, confianza y periodo día/noche. "
    "Después se asignó un nivel de riesgo según el promedio de FRP, brillo térmico y concentración de detecciones."
)

st.dataframe(clusters, width="stretch")

color_riesgo_plot = {
    "Alto": "#d62728",
    "Medio": "#ff7f0e",
    "Bajo": "#1f77b4"
}

fig_clusters = px.bar(
    clusters,
    x="riesgo",
    y="total_incendios",
    color="riesgo",
    color_discrete_map=color_riesgo_plot,
    title="Cantidad de detecciones por nivel de riesgo",
    text="total_incendios",
    category_orders={"riesgo": ["Alto", "Medio", "Bajo"]}
)
st.plotly_chart(fig_clusters, width="stretch")

st.divider()

st.subheader("Mapa de detecciones con clusters de riesgo")

st.write("Para que el dashboard cargue rápido, el mapa usa una muestra aleatoria de 50,000 puntos.")

riesgo_color = {
    "Alto": [220, 40, 40, 160],
    "Medio": [255, 165, 0, 150],
    "Bajo": [40, 120, 220, 130]
}

puntos["color"] = puntos["riesgo"].map(riesgo_color)

layer = pdk.Layer(
    "ScatterplotLayer",
    data=puntos,
    get_position="[longitude, latitude]",
    get_fill_color="color",
    get_radius=2500,
    pickable=True,
    opacity=0.7
)

view_state = pdk.ViewState(
    latitude=23.6345,
    longitude=-102.5528,
    zoom=4,
    pitch=0
)

tooltip = {
    "html": """
    <b>Riesgo:</b> {riesgo}<br/>
    <b>Cluster:</b> {cluster}<br/>
    <b>FRP:</b> {frp}<br/>
    <b>Brightness:</b> {brightness}<br/>
    <b>Fecha:</b> {acq_date}
    """,
    "style": {"backgroundColor": "black", "color": "white"}
}

st.pydeck_chart(
    pdk.Deck(
        map_style=None,
        initial_view_state=view_state,
        layers=[layer],
        tooltip=tooltip
    )
)

st.divider()

st.subheader("Top 10 días con más detecciones")

st.dataframe(top_dias, width="stretch")

fig_top = px.bar(
    top_dias,
    x="acq_date",
    y="total_incendios",
    title="Top 10 días con más incendios detectados",
    text="total_incendios"
)
st.plotly_chart(fig_top, width="stretch")

st.subheader("Zonas aproximadas con más detecciones")

st.dataframe(zonas, width="stretch")

st.caption("Proyecto final de Big Data | NASA FIRMS + PySpark + SparkML + Streamlit")
