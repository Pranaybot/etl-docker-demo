import os
import time
import psycopg2

def main():
    host = os.getenv("DB_HOST", "postgres")
    port = int(os.getenv("DB_PORT", "5432"))
    dbname = os.getenv("DB_NAME", "etl_db")
    user = os.getenv("DB_USER", "etl_user")
    pwd  = os.getenv("DB_PASSWORD", "etl_password")

    print("Python ETL starting...")

    conn=None
    for i in range(10):
	try:
            conn = psycopg2.connect(
                   host=host,port=port,
        	   dbname=dbname, user=user,password=pwd
       	    )
	    print("Connected to Postgres from Python ETL.")
	    break
        except Exception as e:
	    print(f"Python ETL: Waiting for Postgres... ({9 - i} retries left)")
    	    time.sleep(3)
    
    if conn is None:
        print("Python ETL: could not connect to Postgres.")
 	return

    cur = conn.cursor()
    cur.execute(
        """
	CREATE TABLE IF NOT EXISTS python_etl_data (
	    id SERIAL PRIMARY KEY,
	    message TEXT NOT NULL
	)
	"""
    )
    cur.execute(
    	"INSERT INTO python_etl_data (message) VALUES (%s)",
	("Hello from Python ETL",)
    )

    conn.commit()
    cur.close()
    conn.close()

    print("Python ETL inserted a row into python_etl_data.")
    print("Python ETL: Done.")

if __name__ == "__main__":
    main()

