import os
import time
import psycopg2

def get_env(name, default):
    return os.environ.get(name, default)

def main():
    host = get_env("POSTGRES_HOST", "postgres")
    db   = get_env("POSTGRES_DB", "etl_db")
    user = get_env("POSTGRES_USER", "etl_user")
    pwd  = get_env("POSTGRES_PASSWORD", "etl_password")

    print("Python ETL: Waiting for Postgres...")
    time.sleep(15)

    print(f"Python ETL: Connecting to postgres://{host}:5432/{db}")
    conn = psycopg2.connect(
        host=host,
        port=5432,
        dbname=db,
        user=user,
        password=pwd,
    )
    cur = conn.cursor()

    # Read rows inserted by Java ETL and Kafka ETL
    cur.execute("SELECT id, source, payload FROM etl_records ORDER BY id")
    rows = cur.fetchall()

    print("Python ETL: Found rows in etl_records:")
    for r in rows:
        print(f"  id={r[0]}, source={r[1]}, payload={r[2]}")

    cur.close()
    conn.close()
    print("Python ETL: Done.")

if __name__ == "__main__":
    main()

