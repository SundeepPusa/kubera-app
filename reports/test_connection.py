import psycopg2
import os

DB_URL = "postgresql://osa_admin:SiriAWS26@osa-db.c12ccwc88y0j.eu-north-1.rds.amazonaws.com:5432/osa_db"

try:
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()
    cur.execute("SELECT now();")
    print("Connected successfully:", cur.fetchone())
    conn.close()
except Exception as e:
    print("Connection failed:", e)
