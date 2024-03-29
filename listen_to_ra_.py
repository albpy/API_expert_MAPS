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
def trigger_otb_integration(LIBRARY_NAME:str,trigger_table:str,symbol_name:str,ps_conn):

    lib = rf.get_library(LIBRARY_NAME, create_if_missing=True)
    # Fetch RA Transactions
    print(f"[INFO] FETCH RA TRANSACTIONS")
    
    ps_conn.execute("SELECT * from ra_table;")
    ra_data    = ps_conn.fetchall()
    columns = [desc[0] for desc in ps_conn.description]
    ra_df      = pd.DataFrame(ra_data, columns=columns)

    ra_df["Budget_date"]      = pd.to_datetime(ra_df["Budget_date"])
    # ra_df["ENDOFLife"]  = ra_df["ENDOFLife"].fillna("")
    cols_to_conv        = ["budget_qty"]

    # cols_to_conv        = ["ClosingStock", "TotalPurchaseQty", "StockOnHandQty", "UnitsBuyBySku", "BudgetQTY", "OpeningStock"]
    ra_df[cols_to_conv] = ra_df[cols_to_conv].apply(pd.to_numeric)

    lib.write(symbol_name, ra_df,prune_previous_versions=True)
    ra_df = None

    # Load ra_trnx
    q = QueryBuilder()
    ra_trnx: pl.DataFrame = pl.DataFrame(rf.get_library(LIBRARY_NAME).read("ra_trnx", query_builder=q).data)
    print(ra_trnx['budget_amount'].sum(), 'kjkjk')

    # Lod stock trnx
    q = QueryBuilder()
    q = q.apply("total_purchase_qty", q["opening_stock"] + q['stock_received_qty'])
    stock_trnx: pl.DataFrame = pl.DataFrame(rf.get_library(LIBRARY_NAME).read("stock_trnx", query_builder=q).data)
    # stock_trnx = stock_trnx.replace({'StoreID':'INVENTLOCATIONID'})
    print(f"[{ctime()}] [INFO] Applying Joins between RA TRX and stock TRX")

    # Join stock and ra_trnx
    rf.get_library(LIBRARY_NAME).write(
        "ra_stock_joined",
        ra_trnx.join(
            stock_trnx,
            on=["ITEMID", 'INVENTLOCATIONID'],
            how="left"
        ).to_pandas(),prune_previous_versions=True
    )
    q = QueryBuilder()
    ra_stock_joined: pl.DataFrame = pl.DataFrame(rf.get_library(LIBRARY_NAME).read("ra_stock_joined", query_builder=q).data)
    print(ra_stock_joined.shape,"ra_stock_joined")
    print(ra_stock_joined.columns,'stock_columns')

    # Join ra_stock join with kpi_trnx
    q = QueryBuilder()
    print(f"[{ctime()}] [INFO] Applying Joins between ra_stock_joined and kpi_trnx")
    kpi_trnx: pl.DataFrame = pl.DataFrame(rf.get_library(LIBRARY_NAME).read("kpi_trnx", query_builder=q).data)

    rf.get_library(LIBRARY_NAME).write(
        "ra_kpi_joined",
        ra_stock_joined.join(
            kpi_trnx,
            on=["ITEMID"],
            how="left"
        ).to_pandas(),prune_previous_versions=True
    )


    
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
cursor.execute("LISTEN ra_channel;")
print("connection established...")

try:
    while True:
        if select.select([connection],[],[],5) == ([],[],[]):
            print("Timeout")
        else:
            connection.poll()
            while connection.notifies:
                # trigger function invoke
                print("we have notification")
                lib = trigger_otb_integration(LIBRARY_NAME='RA_DATA',trigger_table="ra_table",symbol_name="ra_trnx",ps_conn=cursor)
                break
except:
    print(traceback.format_exc())