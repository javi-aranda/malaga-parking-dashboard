import os
import sqlite3
from dotenv import load_dotenv

load_dotenv()

DB_NAME = os.getenv("DB_NAME")


def create_db():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS parkings(
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            address TEXT NOT NULL,
            latitude REAL NOT NULL,
            longitude REAL NOT NULL,
            altitude REAL NOT NULL,
            total_spaces INTEGER NOT NULL
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS parking_data(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            parking_id INTEGER NOT NULL REFERENCES parkings(id),
            timestamp DATETIME NOT NULL,
            free_spaces INTEGER NOT NULL
        )
    """)
    conn.commit()
    conn.close()


def test_populate_parkings_query():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("SELECT name, total_spaces FROM parkings")
    print(cursor.fetchall())
    conn.close()


def test_populate_parking_data_query():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM parking_data")
    print(cursor.fetchall())
    conn.close()


if __name__ == "__main__":
    create_db()
    # test_populate_parkings_query()
    # test_populate_parking_data_query()
    ...
