import os
import logging
import sqlite3
import polars as pl
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

DB_NAME = os.getenv("DB_NAME")
MALAGA_PARKING_DATA_PATH = os.getenv("MALAGA_PARKING_DATA_PATH")


def populate_parkings():
    path_catalog = os.path.join(MALAGA_PARKING_DATA_PATH, "catalogo.csv")

    df = pl.read_csv(path_catalog)

    # Ajustar plazas de los parkings con información de la web del Ayuntamiento de Málaga
    total_spaces = [436, 135, 621, 458, 532, 702, 450, 262, 440, 261]

    # Añadir columna de total_spaces
    df = df.with_columns(pl.Series("total_spaces", total_spaces, dtype=pl.Int32))

    # Para cada fila del dataframe, insertar en la base de datos
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    for row in df.iter_rows(named=True):
        cursor.execute(
            """
            INSERT INTO parkings(id, name, address, latitude, longitude, altitude, total_spaces)
            VALUES(?, ?, ?, ?, ?, ?, ?)
        """,
            (
                row["id"],
                row["nombre"],
                row["direccion"],
                row["latitude"],
                row["longitude"],
                row["altitud"],
                row["total_spaces"],
            ),
        )

    conn.commit()
    conn.close()


def new_ingest(df):
    # Obtener los parkings de la base de datos
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM parkings")
    parkings = cursor.fetchall()

    # Para cada fila del dataframe, insertar en la base de datos si coincide el ID del parking con la de la base de datos
    for row in df.iter_rows(named=True):
        for parking in parkings:
            if row["id"] == parking[0]:
                cursor.execute(
                    """
                    INSERT INTO parking_data(parking_id, timestamp, free_spaces)
                    VALUES(?, ?, ?)
                """,
                    (parking[0], row["timestamp"], row["libres"]),
                )
                break

    conn.commit()
    conn.close()


def old_ingest(df):
    # Obtener los parkings de la base de datos
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM parkings")
    parkings = cursor.fetchall()

    # Para cada fila del dataframe, insertar en la base de datos si coincide la dirección con la de la base de datos
    for row in df.iter_rows(named=True):
        for parking in parkings:
            if row["direccion"] == parking[2]:
                cursor.execute(
                    """
                    INSERT INTO parking_data(parking_id, timestamp, free_spaces)
                    VALUES(?, ?, ?)
                """,
                    (parking[0], row["timestamp"], row["libres"]),
                )
                break

    conn.commit()
    conn.close()


def ingest_parking_data():
    base_path = os.path.join(MALAGA_PARKING_DATA_PATH, "data")
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    # Último timestamp de la base de datos
    cursor.execute("SELECT MAX(timestamp) FROM parking_data")
    max_timestamp = cursor.fetchone()[0]
    conn.close()

    if max_timestamp:
        max_timestamp = datetime.strptime(max_timestamp, "%Y-%m-%d %H:%M:%S")
    else:
        max_timestamp = (
            datetime.min
        )  # Primera fecha posible (cualquiera previa a mayo 2022 sirve)

    for root, _, files in os.walk(base_path):
        for file in files:
            if file.startswith("parking-data") and file.endswith(".csv"):
                # Descomponer la ruta para obtener el timestamp
                yyyy, mm, dd = os.path.normpath(root).split(os.sep)[-3:]
                hour, minute = file.split(".")[0].split("-")[-1].split("_")
                timestamp_str = f"{yyyy}-{mm}-{dd} {hour}:{minute}:00"
                file_timestamp = datetime(
                    int(yyyy), int(mm), int(dd), int(hour), int(minute)
                )

                # Si el timestamp del archivo es anterior o igual al máximo timestamp de la base de datos, nos lo saltamos
                if file_timestamp <= max_timestamp:
                    continue

                path = os.path.join(root, file)
                try:
                    df = pl.read_csv(path)

                    # Agregar timestamp
                    df = df.with_columns(
                        pl.Series("timestamp", [timestamp_str] * len(df), dtype=pl.Utf8)
                    )

                    if "poiID" in df.columns:
                        logger.debug("Ingesting old data")
                        old_ingest(df)
                    else:
                        logger.debug("Ingesting new data")
                        new_ingest(df)

                except pl.exceptions.NoDataError:
                    logger.warning(f"No data in file {path}, skipping...")


if __name__ == "__main__":
    # populate_parkings()
    ingest_parking_data()
    ...
