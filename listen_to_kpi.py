import psycopg2
import subprocess
import traceback
import psycopg2.extensions
import select



from sqlalchemy import Column, Integer, Numeric, String, Date, Index, Text, text, Float, create_engine  # The architects of our database schema, crafting the structure of our data haven.
from sqlalchemy.orm import sessionmaker, declarative_base  # Craftsmen who build the bridge between our Pythonic realm and the database underworld.
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import declarative_base, sessionmaker
from rapidframes import RapiDF, QueryBuilder
from time import ctime

import threading
import warnings
from typing import List, Dict
import polars as pl
import pandas as pd



# storage url
STORAGE_URL = 'lmdb://databases/bmaps_data?map_size=50GB'
rf = RapiDF(STORAGE_URL)


# function to invoke
def trigger_kpi_integration(LIBRARY_NAME:str,trigger_table:str,symbol_name:str,ps_conn):

    lib = rf.get_library(LIBRARY_NAME, create_if_missing=True)
    # Fetch RA Transactions
    print(f"[INFO] FETCH KPI TRANSACTIONS")
    
    ps_conn.execute("SELECT * from kpi_table;")
    ra_data    = ps_conn.fetchall()
    columns = [desc[0] for desc in ps_conn.description]
    ra_df      = pd.DataFrame(ra_data, columns=columns)

    lib.write(symbol_name, ra_df,prune_previous_versions=True)
    ra_df = None

    print(lib.list_versions(), 'the symbol version')
    # Load ra_trnx
    q = QueryBuilder()
    ra_trnx: pl.DataFrame = pl.DataFrame(rf.get_library(LIBRARY_NAME).read("ra_trnx", query_builder=q).data)
    print(ra_trnx.shape,"ra_shape")

    # Lod stock trnx
    q = QueryBuilder()
    q = q.apply("total_purchase_qty", q["opening_stock"] + q['stock_received_qty'])
    stock_trnx: pl.DataFrame = pl.DataFrame(rf.get_library(LIBRARY_NAME).read("stock_trnx", query_builder=q).data)
    print(f"[{ctime()}] [INFO] Applying Joins between RA TRX and stock TRX")

   # Join stock and ra_trnx
    rf.get_library(LIBRARY_NAME).write(
        "ra_stock_joined",
        ra_trnx.join(
            stock_trnx,
            on=["ITEMID"],
            how="left"
        ).to_pandas(),prune_previous_versions=True
    )
    q = QueryBuilder()
    ra_stock_joined: pl.DataFrame = pl.DataFrame(rf.get_library(LIBRARY_NAME).read("ra_stock_joined", query_builder=q).data)
    print(ra_stock_joined.shape,"ra_stock_joined")

    # Join ra_stock join with kpi_trnx
    q = QueryBuilder()
    print(f"[{ctime()}] [INFO] Applying Joins between ra_stock_joined and kpi_trnx")
    # c='xxxx'
    # print(f"{c}")
    try:
        q = QueryBuilder()
        kpi_trnx: pl.DataFrame = pl.DataFrame(rf.get_library(LIBRARY_NAME).read("kpi_trnx", query_builder=q).data)
    
        rf.get_library(LIBRARY_NAME).write(
            "ra_kpi_joined",
            ra_stock_joined.join(
                kpi_trnx,
                on=["ITEMID"],
                how="left"
            ).to_pandas(),prune_previous_versions=True
        )
    except:
        print(traceback.format_exc())
    return lib



connection = psycopg2.connect(
host =  "35.154.160.43",
dbname = "bmaps",
user = "postgres",
password = "6eA1Wyp6l'sj",
port    = "5432"

)

connection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
print("trying to connect...")
cursor = connection.cursor()
# Listen to the channel sp_channel
cursor.execute("LISTEN kpi_channel;")
print("connection established...")


while True:
    if select.select([connection],[],[],5) == ([],[],[]):
        print("Timeout")
    else:
        connection.poll()
        while connection.notifies:
            print("we have notification")
            for notification in connection.notifies:
                print("Received notification on channel:", notification.channel)
                print("Payload:", notification.payload)
            # trigger function invoke
            lib = trigger_kpi_integration(LIBRARY_NAME='RA_DATA',trigger_table="kpi_table",symbol_name="kpi_trnx",ps_conn=cursor)
            break

