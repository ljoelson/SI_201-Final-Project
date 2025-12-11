import sqlite3

DB_NAME = "project_data.db"

conn = sqlite3.connect(DB_NAME)
cursor = conn.cursor()

# Get all tables
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = cursor.fetchall()

print("=" * 60)
print("DATABASE CONTENTS CHECK")
print("=" * 60)
print(f"\nDatabase: {DB_NAME}")
print(f"\nTables found: {len(tables)}")
print()

if len(tables) == 0:
    print("⚠️  No tables found! Database is empty.")
else:
    for table in tables:
        table_name = table[0]
        print(f"✓ Table: {table_name}")
        
        # Count rows in each table
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        print(f"  Rows: {count}")
        
        # Show schema
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns = cursor.fetchall()
        print(f"  Columns: {[col[1] for col in columns]}")
        print()

conn.close()

print("=" * 60)
print("\nIf you see only one table, try:")
print("1. Delete project_data.db")
print("2. Run: python flights_api.py")
print("3. Run: python check_database.py")
print("4. Run: python weather_api.py")
print("5. Run: python check_database.py again")
print("=" * 60)