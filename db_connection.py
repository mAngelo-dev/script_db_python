from dotenv import load_dotenv
import psycopg2
import os

load_dotenv()

def connect():
    conn = psycopg2.connect(
        host=os.environ['POSTGRES_HOST'] or 'localhost',
        port=os.environ['POSTGRES_PORT'] or '5432',
        database=os.environ['POSTGRES_DATABASE'] or 'postgres',
        user=os.environ['POSTGRES_USER'] or 'postgres',
        password= os.environ['POSTGRES_PASSWORD']
    )
    return conn
