import requests
import sqlite3
import datetime
import time

OPENWEATHER_KEY = "2aee95c86e2cf484f13b865933689b2a"
CITY = "Detroit"
LAT = 42.3314
LON = -83.0458
max_rows = 25

def get_sql_connection():
    conn = sqlite3.connect("dawgs_project.db")
    return conn