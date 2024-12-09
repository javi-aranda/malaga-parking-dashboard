import os
import logging
import streamlit as st
import sqlite3
import polars as pl
import plotly.express as px
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

DB_NAME = os.getenv("DB_NAME")
TTL = 60 * 30

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

@st.cache_resource
def get_db_connection():
    """Crea conexión a la base de datos."""
    logger.info(f"Conectando a la base de datos {DB_NAME}")
    return sqlite3.connect(DB_NAME)

@st.cache_resource
def get_all_parkings():
    """Devuelve todos los parkings."""
    conn = get_db_connection()
    query = "SELECT id, name, total_spaces FROM parkings"
    parkings = pl.read_database(query, conn)
    conn.close()
    return parkings

@st.cache_resource(ttl=TTL)
def get_parking_occupancy(parking_id=None, start_date=None, end_date=None):
    """
    Recuperar información de los parkings con filtros opcionales.

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


def main():
    st.set_page_config(page_title='Málaga Parking Dashboard', page_icon="🅿️", layout="centered")

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
            "Ocupación en el tiempo",
            "Comparación de parkings",
            "Resumen de estadísticas",
            "Sobre el proyecto",
        ]
    )

    with tab1:
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

    with tab2:
        st.header("Comparación de parkings")

        if not occupancy_data.is_empty():
            fig = px.box(
                occupancy_data,
                x="name",
                y="occupancy_percentage",
                title="Distribución de ocupación por parking",
                labels={"occupancy_percentage": "Ocupación (%)", "name": "Parking"},
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("Sin datos disponibles para comparar.")

    with tab3:
        start_datetime_fmt = start_datetime.strftime('%Y/%m/%d')
        end_datetime_fmt = end_datetime.strftime('%Y/%m/%d')
        
        st.header(f"Resumen de estadísticas entre {start_datetime_fmt} y {end_datetime_fmt}")
        
        # Resumen de estadísticas'
        if not occupancy_data.is_empty():
            summary_stats = occupancy_data.group_by("name").agg(
                [
                    pl.col("free_spaces")
                    .mean()
                    .round(0)
                    .alias("Media de espacios libres"),
                    pl.col("occupancy_percentage")
                    .mean()
                    .round(2)
                    .alias("% Medio ocupación"),
                    pl.col("occupancy_percentage")
                    .median()
                    .round(2)
                    .alias("% Mediana ocupación"),
                ]
            )

            st.dataframe(summary_stats)
        else:
            st.warning("No hay resumen de estadísticas disponible.")

    with tab4:
        st.header("Sobre el proyecto")

        st.markdown(read_file_contents("README.md"))


if __name__ == "__main__":
    main()
