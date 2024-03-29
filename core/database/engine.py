from decouple import config
from dotenv import load_dotenv
from sqlalchemy import create_engine
import os

load_dotenv()

host          = os.environ.get("DB_URL")
database_name = os.environ.get("DB_NAME")
username      = os.environ.get("DB_USER")
password      = os.environ.get("DB_PASSWORD")

# PostgreSQL database configuration
SQLALCHEMY_DATABASE_URL = f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database_name}"  # POSTGRES

engine = create_engine(SQLALCHEMY_DATABASE_URL)
