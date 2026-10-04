import os
import logging
import streamlit as st
import sqlite3
import folium
import polars as pl
import plotly.express as px
from datetime import datetime, timedelta
from dotenv import load_dotenv
from streamlit_folium import st_folium

load_dotenv()

logger = logging.getLogger(__name__)

DB_NAME = os.getenv("DB_NAME")
TTL = 60 * 30
MALAGA_COORDS = 36.7213, -4.4216

def read_file_contents(file_path):
    """Wrapper para leer ficheros.

    Args:
      file_path (str): Ruta del fichero.

    Returns:
      str: Cadena con los contenidos.
    """

    with open(file_path, "r", encoding="utf-8") as f:
        content = f.read()
    return content

def get_db_connection():
    """Crea conexión a la base de datos."""
    logger.info(f"Conectando a la base de datos {DB_NAME}")
    return sqlite3.connect(DB_NAME)

def get_all_parkings():
    """Devuelve todos los parkings."""
    conn = get_db_connection()
    query = "SELECT id, name, total_spaces FROM parkings"
    parkings = pl.read_database(query, conn)
    conn.close()
    return parkings

def get_parking_occupancy(parking_id=None, start_date=None, end_date=None):
    """
    Recupera información de los parkings con filtros opcionales.

    Args:
        parking_id (str, optional): ID del parking
        start_date (datetime, optional): Fecha inicial
        end_date (datetime, optional): Fecha final

    Returns:
        polars.DataFrame: Datos de ocupación del parking
    """
    conn = get_db_connection()

    # Query básica
    query = """
    SELECT pd.parking_id, p.name, pd.timestamp, pd.free_spaces, 
           p.total_spaces, 
           (p.total_spaces - pd.free_spaces) AS occupied_spaces,
           ((p.total_spaces - pd.free_spaces) * 100 / p.total_spaces) AS occupancy_percentage
    FROM parking_data pd
    JOIN parkings p ON pd.parking_id = p.id
    WHERE 1=1
    """

    # Gestión de filtros
    conditions = []

    if parking_id:
        conditions.append(f"pd.parking_id = '{parking_id}'")

    if start_date:
        conditions.append(
            f"pd.timestamp >= '{start_date.strftime('%Y-%m-%d %H:%M:%S')}'"
        )

    if end_date:
        conditions.append(f"pd.timestamp <= '{end_date.strftime('%Y-%m-%d %H:%M:%S')}'")

    # Aplicar filtros a la query
    if conditions:
        query += " AND " + " AND ".join(conditions)

    df = pl.read_database(query, conn)
    conn.close()

    # Convertir la columna de timestamp a tipo datetime
    df = df.with_columns(pl.col("timestamp").str.strptime(pl.Datetime))

    return df

@st.cache_data(ttl=TTL, show_spinner=False)
def get_latest_occupancy():
    """Devuelve la última ocupación de cada parking."""
    conn = get_db_connection()
    query = """
    WITH latest_timestamps AS (
        SELECT parking_id, MAX(timestamp) AS max_timestamp
        FROM parking_data
        GROUP BY parking_id
    )
    SELECT pd.parking_id, p.name, p.latitude, p.longitude, 
           ((p.total_spaces - pd.free_spaces) * 100 / p.total_spaces) AS occupancy_percentage
    FROM parking_data pd
    JOIN latest_timestamps lt ON pd.parking_id = lt.parking_id AND pd.timestamp = lt.max_timestamp
    JOIN parkings p ON pd.parking_id = p.id
    """
    latest_occupancy = pl.read_database(query, conn)
    conn.close()
    return latest_occupancy

def get_color(occupancy_percentage):
    """Devuelve el color según el porcentaje de ocupación."""
    if occupancy_percentage <= 50:
        return 'green'
    elif occupancy_percentage <= 70:
        return 'yellow'
    elif occupancy_percentage <= 85:
        return 'orange'
    elif occupancy_percentage <= 95:
        return 'red'
    else:
        return 'purple'

def create_map(latest_occupancy):
    """Crea un mapa de Málaga con las áreas circulares de ocupación."""
    m = folium.Map(location=MALAGA_COORDS, zoom_start=13.5)

    for row in latest_occupancy.iter_rows(named=True):
        folium.Circle(
            location=[row["latitude"], row["longitude"]],
            radius=100,
            color=get_color(row["occupancy_percentage"]),
            fill=True,
            fill_color=get_color(row["occupancy_percentage"]),
            fill_opacity=0.6,
            popup=f"{row['name']}: {row['occupancy_percentage']:.2f}%"
        ).add_to(m)

    return m


def main():
    st.set_page_config(page_title='Málaga Parking Dashboard', page_icon="🅿️", layout="wide")

    st.title("Málaga Parking Dashboard")

    st.sidebar.header("Filtros")

    # Selección de parking
    parkings = get_all_parkings()
    parking_options = ["Todos los parkings"] + list(parkings["name"])
    selected_parking = st.sidebar.selectbox("Selección de parking", parking_options)

    # Selección de fechas
    col1, col2 = st.sidebar.columns(2)
    with col1:
        start_date = st.date_input(
            "Fecha inicial",
            value=datetime.now() - timedelta(days=1),
            max_value=datetime.now(),
        )
    with col2:
        end_date = st.date_input(
            "Fecha final", value=datetime.now(), max_value=datetime.now()
        )

    start_datetime = datetime.combine(start_date, datetime.min.time())
    end_datetime = datetime.combine(end_date, datetime.max.time())

    # Validar fechas
    if start_datetime > end_datetime:
        st.error("La fecha inicial no puede ser mayor a la fecha final.")
        return

    # Filtrar datos
    parking_id = None
    if selected_parking != "Todos los parkings":
        parking_id = parkings.filter(pl.col("name") == selected_parking)["id"][0]

    occupancy_data = get_parking_occupancy(
        parking_id=parking_id, start_date=start_datetime, end_date=end_datetime
    )

    # Tabs de contenido
    tab1, tab2, tab3, tab4 = st.tabs(
        [
            "🗺️ Mapa",
            "📊 Ocupación en el tiempo",
            "📋 Resumen",
            "ℹ️ Sobre el proyecto",
        ]
    )

    with tab1:
        st.header("Mapa de ocupación actual")


        latest_occupancy = get_latest_occupancy()
        if not latest_occupancy.is_empty():

            # Layout en dos columnas
            col1, col2 = st.columns([9, 3])

            with col1:
                display_map(latest_occupancy)

            with col2:
                st.markdown("""
                    ### Leyenda
                    🟢 Ocupación baja
                            
                    🟡 Ocupación media
                            
                    🟠 Ocupación alta
                            
                    🔴 Ocupación muy elevada
                            
                    🟣 Ocupación máxima
                """)
        else:
            st.warning("No hay datos de ocupación disponibles.")

    with tab2:
        st.header("Ocupación a lo largo del tiempo")

        if not occupancy_data.is_empty():
            # Agregar datos de ocupación promedio por hora y parking
            grouped_data = (
                occupancy_data.group_by(["timestamp", "name"])
                .agg(pl.col("occupancy_percentage").mean())
                .sort("timestamp")
            )

            fig = px.line(
                grouped_data,
                x="timestamp",
                y="occupancy_percentage",
                color="name",
                title="Ocupación en el tiempo",
                labels={
                    "occupancy_percentage": "Ocupación (%)",
                    "timestamp": "Fecha y hora",
                },
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("Sin datos disponibles para el periodo seleccionado.")

    with tab3:
        start_datetime_fmt = start_datetime.strftime('%Y/%m/%d')
        end_datetime_fmt = end_datetime.strftime('%Y/%m/%d')

        st.header(f"Resumen entre {start_datetime_fmt} y {end_datetime_fmt}")
        
        # Resumen de estadísticas'
        if not occupancy_data.is_empty():
            summary_stats = occupancy_data.group_by("name").agg(
                [
                    pl.col("total_spaces").first().alias("Plazas totales"),
                    pl.col("free_spaces")
                    .mean()
                    .round(0)
                    .alias("Media espacios libres"),
                    pl.col("occupied_spaces")
                    .mean()
                    .round(0)
                    .alias("Media espacios ocupados"),
                    pl.col("occupancy_percentage")
                    .mean()
                    .round(2)
                    .alias("% Medio ocupación"),
                    pl.col("occupancy_percentage")
                    .median()
                    .round(2)
                    .alias("% Mediana ocupación"),
                    pl.col("occupancy_percentage")
                    .max()
                    .round(2)
                    .alias("% Máx ocupación"),
                    pl.col("occupancy_percentage")
                    .min()
                    .round(2)
                    .alias("% Mín ocupación"),
                    pl.col("free_spaces")
                    .min()
                    .alias("Mínimo espacios libres"),
                    pl.col("free_spaces")
                    .max()
                    .alias("Máximo espacios libres"),
                ]
            )

            st.dataframe(summary_stats, use_container_width=True)
            
            # Métricas adicionales de análisis temporal
            st.subheader("Análisis temporal")
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                # Número total de registros por parking
                st.metric(
                    "Total de registros en el período", 
                    f"{occupancy_data.height:,}"
                )
                
            with col2:
                # Parking más ocupado en promedio
                parking_mas_ocupado = summary_stats.filter(
                    pl.col("% Medio ocupación") == pl.col("% Medio ocupación").max()
                )["name"][0]
                ocupacion_maxima = summary_stats.filter(
                    pl.col("% Medio ocupación") == pl.col("% Medio ocupación").max()
                )["% Medio ocupación"][0]
                st.metric(
                    "Parking más ocupado (promedio)",
                    parking_mas_ocupado,
                    f"{ocupacion_maxima}%"
                )
                
            with col3:
                # Parking menos ocupado en promedio
                parking_menos_ocupado = summary_stats.filter(
                    pl.col("% Medio ocupación") == pl.col("% Medio ocupación").min()
                )["name"][0]
                ocupacion_minima = summary_stats.filter(
                    pl.col("% Medio ocupación") == pl.col("% Medio ocupación").min()
                )["% Medio ocupación"][0]
                st.metric(
                    "Parking menos ocupado (promedio)",
                    parking_menos_ocupado,
                    f"{ocupacion_minima}%"
                )
        else:
            st.warning("No hay resumen de estadísticas disponible.")
    
    with tab4:
        st.markdown(read_file_contents("README.md"))


@st.fragment
def display_map(latest_occupancy):
    m = create_map(latest_occupancy)
    st_folium(m, width=1000, height=500)


if __name__ == "__main__":
    main()
