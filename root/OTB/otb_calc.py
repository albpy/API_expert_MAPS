import datetime
from collections import namedtuple
from datetime import datetime, timedelta, date
from time import ctime
from typing import Dict, List

import pandas as pd
import numpy as np
import polars as pl

from rapidframes import QueryBuilder, RapiDF
# from routes.RA.parameters import Parameters
from routes.otb.parameters import OTB
from warnings import simplefilter 
# sets up a filter to ignore the specified PerformanceWarning category
simplefilter(action="ignore", category=pd.errors.PerformanceWarning)

STORAGE_URL  = 'lmdb://databases/bmaps_data?map_size=50GB'
LIBRARY_NAME = "RA_DATA"

rf = RapiDF(STORAGE_URL)
lib = rf.get_library('RA_DATA', create_if_missing=True)

history_start_date_str  = "2021-01-01"
history_end_date_str    = "2023-12-16"

forecast_start_date_str = "2024-01-01"
forecast_end_date_str   = "2024-12-31"

class symbols:
    # Tables requisited mandatory by OTB from DB 
    RA_TRNX         = "ra_kpi_joined"
    SALES_TRNX      = "cst_trnx"
    filtered_symobl = "filtered_symobl"
    SALES_FILTERED  = "sales_filtered"
    # AP_TRNX       =  "otb_trnx"
    RA_SALES_JOIN   = "ra_sales_join"
    KPI_TRNX        = "kpi_trnx"
    #RA_OTB_JOIN    =  "ra_otb_join"


def filter_symbol(rf: RapiDF, library_name: str, symbol_name: str, columns: List[str]=None, filter_set: Dict[str, str | int | list]= {}, return_as_pl: bool=True):
    # Initialize the QueryBuilder
    q = QueryBuilder()

    # Apply filters dynamically
    for column, conditions in filter_set.items():
        for operator, value in conditions.items():
            if operator == "_gt_": # Greater Than
                q = q[(q[column] > value)]
            elif operator == "_gte_": # Greater than or Equal To
                q = q[(q[column] >= pd.to_datetime(value))]
            elif operator == "_lt_": # Less Than
                q = q[(q[column] < value)]
            elif operator == "_lte_": # Less Than or Equal To
                q = q[(q[column] <= pd.to_datetime(value))]
            elif operator == "_eq_": # Equal To
                q = q[(q[column] == value)]
            elif operator == "_isin_": # Is in the list
                q = q[(q[column].isin(value))]

    filtered_df = rf.get_library(library_name).read(symbol_name, columns=columns, query_builder=q).data
    return pl.DataFrame(filtered_df) if return_as_pl else filtered_df

def form_intervals_by_year(start_date, stop_date):
    start_date = datetime.strptime(start_date, "%Y-%m-%d")
    stop_date = datetime.strptime(stop_date, "%Y-%m-%d")
    intervals = []
    cur_year = datetime.now().year
    l_year = cur_year - 1
    ll_year = l_year - 1
    years = [cur_year, l_year, ll_year]

    for year in years:
        current_date = start_date
        while current_date <= stop_date:
            if current_date.year == year:
                interval_start = current_date
                while current_date.year == year and current_date <= stop_date:
                    current_date += timedelta(days=1)
                interval_end = current_date - timedelta(days=1)
                intervals.append(
                    (
                        interval_start.strftime("%Y-%m-%d"),
                        interval_end.strftime("%Y-%m-%d"),
                    )
                )
            else:
                current_date += timedelta(days=1)

    return intervals, years

def get_sales_year(historical_start_date, historical_stop_date):
    st = []
    interval_mapObj = namedtuple("interval_mapObj", ["start_date", "end_date", "display_names"])
    intervals, years = form_intervals_by_year(
        historical_start_date, historical_stop_date
    )
    interval_map = {}
    for interval in intervals:
        dt = datetime.strptime(interval[0], "%Y-%m-%d")
        interval_map[dt.year] = interval

    for year in years:
        if year not in interval_map:
            interval_map[year] = 0

    labels = dict(
        zip(
            years,
            [
                ["NetSaleY", "CostOfGoodsY", "SoldQtyY", "GrossSalesY"],
                ["NetSaleLY", "CostOfGoodsLY", "SoldQtyLY", "GrossSalesLY"],
                ["NetSaleLLY", "CostOfGoodsLLY", "SoldQtyLLY", "GrossSalesLLY"],
            ],
        )
    )
    interval_map = dict(sorted(interval_map.items()))
    
    for key, val in interval_map.items():
        # print(labels)
        # print(key)
        st.extend(
            [
                interval_mapObj(
                    None, None, []
                ) 
            ] if val == 0 else [
                interval_mapObj(
                    interval_map[key][0], 
                    interval_map[key][1], 
                    [labels[key][i]for i in range(len(labels[key]))]
                ) 
            ]
        )

    return st
# def get_last_stock(filters):
#     start_of_budget_date = filters.forecast_date_range.fro
#     q=QueryBuilder()
#     stock_trnx = pl.DataFrame(rf.get_library(LIBRARY_NAME).read('stock_trnx', query_builder=q).data)
#     # opening_stock_at_that_day = 
#     return None
def create_ra(forecast_start_date_str,forecast_end_date_str,history_start_date_str,history_end_date_str, filters):
    """ Filter out the Range Arch Trx Date on the basis of forecast date range """
    print(f"[{ctime()}] [INFO] filtering RA Transactions")

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~To filter data from rapid frame~~~~~~~~~~~~~~    
    filters_map = {"Channel":filters.sales_channel,"Family":filters.product_family,
            "SubFamily":filters.sub_families,"ITEMID":filters.sku,"Category":filters.category,
            "SubCategory":filters.sub_category,"Supplier":filters.suppliers}
    
#@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ Filtering RA_TRNX
    #first time write, then append
    lib.write("filtered_symobl",filter_symbol(
        rf = rf,
        library_name=LIBRARY_NAME, symbol_name=symbols.RA_TRNX,
        filter_set={
           "Budget_date":   {"_gte_": filters.forecast_date_range.fro, "_lte_": filters.forecast_date_range.to}
        },return_as_pl=False
    )
  )


    for key,val in filters_map.items():
        if val != []:
            rf.get_library(LIBRARY_NAME).write(symbols.filtered_symobl,filter_symbol(
                rf = rf,
                library_name=LIBRARY_NAME, symbol_name=symbols.filtered_symobl,
                filter_set={
                    f"{key}" : {"_isin_": val},
                },return_as_pl=False)
            )

    q = QueryBuilder()
    filtered_ra_trx =  pl.DataFrame(rf.get_library(LIBRARY_NAME).read('filtered_symobl', query_builder=q).data)
    print(filtered_ra_trx.columns, 'colsss_ra')
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#-------------------------To take total data without primary filters-------------------------------------    
    
    # filtered_ra_trx: pl.DataFrame = filter_symbol(
    #     rf = rf,
    #     library_name=LIBRARY_NAME, symbol_name=symbols.RA_TRNX,
    #     filter_set={
    #         "BDate":   {"_gte_": forecast_start_date_str, "_lte_": forecast_end_date_str}
    #     }
    # )
#--------------------------------------------------------------------
#@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ Filtering SALES_TRNX
    # Get unique Channels
    unique_channels: List[str] = list(filtered_ra_trx["Channel"].unique())
    # Get Unique Item IDs
    unique_item_ids: List[str] = list(filtered_ra_trx["ITEMID"].unique())
    # Get Unique Store IDs
    unique_store_ids: List[str] = list(filtered_ra_trx["INVENTLOCATIONID"].unique())

    
    print(f"[{ctime()}] [INFO] filtering sales transactions")
# channel taken from stores not in sales
# itemmaster --> family, subfamily, supplier
    rf.get_library(LIBRARY_NAME).write(
        symbols.SALES_FILTERED,
        filter_symbol(
            rf = rf,
            library_name=LIBRARY_NAME, symbol_name="cst_trnx",
            filter_set={
                "INVOICEDATE"     : {"_gte_": history_start_date_str, "_lte_": history_end_date_str},
                "channel"         : {"_isin_": unique_channels},
                "ITEMID"          : {"_isin_": unique_item_ids},
                "INVENTLOCATIONID": {"_isin_": unique_store_ids}
            },
            return_as_pl=False
        ),
        prune_previous_versions=True
    )


    q = QueryBuilder()
    q = q.apply("AverageRetailPrice", q["gross_sales"] / q["SALESQTY"])
    q = q.apply("FinalPrice", q["LINEAMOUNT"] / q["SALESQTY"])

    filtered_sales_trx: pl.DataFrame = pl.DataFrame(rf.get_library(LIBRARY_NAME).read("sales_filtered", query_builder=q).data)
    filtered_sales_trx = filtered_sales_trx.rename({'channel' : 'Channel'})
    print(filtered_sales_trx.columns,"hedd")
#@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ Filtering STOCK_TRNX

# {"Channel":filters.sales_channel,"Family":filters.product_family,
#             "SubFamily":filters.sub_families,"ITEMID":filters.sku,"Category":filters.category,
#             "SubCategory":filters.sub_category,"Supplier":filters.suppliers}


    print(f"[{ctime()}] [INFO] Applying Joins between Filtered RA TRX and Filtered Sales TRX")
    
#@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ TODO Join
    
    
    rf.get_library(LIBRARY_NAME).write(
        symbols.RA_SALES_JOIN,
        filtered_ra_trx.join(
            filtered_sales_trx,
            on=["ITEMID", "Channel", "INVENTLOCATIONID"], #
            how="left"
        ).to_pandas()
    )
#@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@

    # Divide the merged table on three years
    # date_wise_columns = get_sales_year(historical_start_date=history_start_date_str, historical_stop_date=history_end_date_str)


    datas = rf.get_library(LIBRARY_NAME).read(symbols.RA_SALES_JOIN,query_builder=q).data
    print(type(datas),"rapid frame")
    return datas


#---------------------------////------||------////--------------------------------------------


def form_base_data(df:pd.DataFrame, filters) -> pl.DataFrame:

    
    current_datetime = datetime.now()
    cur_year = str(current_datetime.year)
    l_year = str(int(cur_year)-1)
    ll_year = str(int(l_year)-1)

    
    df = pl.from_pandas(df)

    print(round(df.estimated_size('mb'),2)," MB memory size of data")
    print(round(df.estimated_size('gb'),2)," GB memory size of data")
    df = df.with_columns((df['historical_year'].fill_nan(0).cast(pl.Int16).cast(pl.Utf8).fill_null("unknown")))
    df = df.with_columns((df['history_quarter'].fill_null("unknown")))
    df = df.with_columns((df['history_month'].fill_null("")))
    df = df.with_columns((df['history_week'].fill_null("unknown")))
    df = df.with_columns((df['history_day'].fill_null("unknown")))
    df = df.with_columns((df["INVOICEDATE"].cast(pl.Date).cast(pl.Utf8).fill_null("unknown")))  
    df = df.with_columns((df["Budget_date"].cast(pl.Date).cast(pl.Utf8)))  

   

    df = df.with_columns((pl.when(df['historical_year']==cur_year)
                                    .then(df['SALESQTY'])
                                    .otherwise(0)).alias('quantity_actuals'))
    df = df.with_columns((pl.when(df['historical_year']==l_year)
                                    .then(df['SALESQTY'])
                                    .otherwise(0)).alias('sold_qty_ly'))
    df = df.with_columns((pl.when(df['historical_year']==ll_year)
                                    .then(df['SALESQTY'])
                                    .otherwise(0)).alias("quantity_ppy"))

    
    df = df.with_columns((pl.when(df['historical_year']==cur_year)
                                    .then(df['LINEAMOUNT'])
                                    .otherwise(0)).alias('sales_actual'))
    df = df.with_columns((pl.when(df['historical_year']==l_year)
                                    .then(df['LINEAMOUNT'])
                                    .otherwise(0)).alias("net_sales_ly"))
    df = df.with_columns((pl.when(df['historical_year']==ll_year)
                                    .then(df['LINEAMOUNT'])
                                    .otherwise(0)).alias("net_sales_lly"))
    
        
    df = df.with_columns((pl.when(df['historical_year']==cur_year)
                                    .then(df['COSTPRICE'])
                                    .otherwise(0)).alias('cost_actuals'))
    df = df.with_columns((pl.when(df['historical_year']==l_year)
                                    .then(df['COSTPRICE'])
                                    .otherwise(0)).alias('cost_of_goods_ly'))
    df = df.with_columns((pl.when(df['historical_year']==ll_year)
                                    .then(df['COSTPRICE'])
                                    .otherwise(0)).alias('cost_of_goods_lly'))
    
    df = df.with_columns((pl.when(df['historical_year']==cur_year)
                                    .then(df['gross_sales'])
                                    .otherwise(0)).alias('gross_sales_ty'))
    df = df.with_columns((pl.when(df['historical_year']==l_year)
                                    .then(df['gross_sales'])
                                    .otherwise(0)).alias('gross_sales_ly'))
    df = df.with_columns((pl.when(df['historical_year']==ll_year)
                                    .then(df['gross_sales'])
                                    .otherwise(0)).alias('gross_sales_lly'))
    
    df = df.with_columns((pl.when(df['historical_year'] == cur_year)
                                    .then(df['ITEMID'])
                                    .otherwise(0)).alias('ITEMID_ty'))
    df = df.with_columns((pl.when(df['historical_year'] == l_year)
                                    .then(df['ITEMID'])
                                    .otherwise(0)).alias('ITEMID_ly'))
    df = df.with_columns((pl.when(df['historical_year'] == ll_year)
                                    .then(df['ITEMID'])
                                    .otherwise(0)).alias('ITEMID_lly'))
    
    df = df.with_columns((pl.when(df['historical_year'] == cur_year)
                                    .then(df['budget_qty'])
                                    .otherwise(0)).alias('budget_qty_ty'))
    df = df.with_columns((pl.when(df['historical_year'] == l_year)
                                    .then(df['budget_qty'])
                                    .otherwise(0)).alias('budget_qty_ly'))
    df = df.with_columns((pl.when(df['historical_year'] == ll_year)
                                    .then(df['budget_qty'])
                                    .otherwise(0)).alias('budget_qty_lly'))
    
    # df = df.with_columns((pl.when(df['historical_year'] == cur_year)
    #                                 .then(df['budget_cost'])
    #                                 .otherwise(0)).alias('budget_cost_ty'))
    # df = df.with_columns((pl.when(df['historical_year'] == l_year)
    #                                 .then(df['budget_cost'])
    #                                 .otherwise(0)).alias('budget_cost_ly'))
    # df = df.with_columns((pl.when(df['historical_year'] == ll_year)
    #                                 .then(df['budget_cost'])
    #                                 .otherwise(0)).alias('budget_cost_lly'))
    print(df.columns, 'later fields need to be calculated')
    # df = df.with_columns((plcol))
    
    df = df.with_columns(current_stock_cost = pl.lit(0).cast(pl.Float64))
    
    # df = df.with_columns(opening_stock = pl.lit(0))
    # df = df.with_columns(stock_received_qty = pl.lit(0))
    # df = df.with_columns(closing_stock = pl.lit(0))
    # df = df.with_columns(stock_date = pl.lit(0))
    # df = df.with_columns(stock_on_hand_qty = pl.lit(0))






    df = df.with_columns((pl.when(df['historical_year'] == l_year)
                                    .then(df['current_stock_cost'])
                                    .otherwise(0)).alias('stock_cost_ly'))
    
    


    # df['quantity_actuals'] = df.loc[,'SALESQTY']
    # df['sold_qty_ly'] = df.loc[df['historical_year']==l_year,'SALESQTY']
    # df["quantity_ppy"] = df.loc[df['historical_year']==ll_year,'SALESQTY']

    # df['sales_actual'] = df.loc[df['historical_year']==cur_year,'LINEAMOUNT']
    # df["net_sales_ly"] = df.loc[df['historical_year']==l_year,'LINEAMOUNT']
    # df["net_sales_lly"] = df.loc[df['historical_year']==ll_year,'LINEAMOUNT']
    # print(df["net_sales_ly"].sum(),"netsasalee  e")

    # df['cost_actuals'] = df.loc[df['historical_year']==cur_year,'COSTPRICE']
    # df['cost_of_goods_ly'] = df.loc[df['historical_year']==l_year,'COSTPRICE']
    # df['cost_of_goods_lly'] = df.loc[df['historical_year']==ll_year,'COSTPRICE']
    # df['gross_sales_ly'] = df.loc[df['historical_year']==l_year,'gross_sales']

    # df['net_sales_mix_percent'] = (df["net_sales_ly"]/sums)*100
    # df["final_price"] = df["net_sales_ly"]/df['sold_qty_ly']
    # df["initial_average_retail_price"] = df['gross_sales_ly']/df['sold_qty_ly']

    # df["purchase_value"] = (df['OpeningStock']+df['StockReceivedQty'])*df["initial_average_retail_price"] 
    # df["sales_act_forecast"] = df['sales_actual']

    # df['purchase_value_mix_percent'] = (df["purchase_value"]/sums)*100
    # df["budget_percent"] = (df['BudgetAmount']/sums)*100
    #  df["relative_budget_percent"] = df["budget_percent"]

    df = df.rename({'opening_stock':'OpeningStock', 'stock_received_qty':'StockReceivedQty', 'closing_stock':'ClosingStock', 
                    'budget_amount':'BudgetAmount', 'budget_cost':'BudgetCost'})

    sums = df["net_sales_ly"].sum()
    df = df.with_columns(((df["net_sales_ly"]/sums)*100).alias('net_sales_mix_percent')) # -->

    df = df.with_columns((df["net_sales_ly"]/df['sold_qty_ly']).alias("final_price"))
    df = df.with_columns((df['gross_sales_ly']/df['sold_qty_ly']).alias("initial_average_retail_price"))

    df = df.with_columns((df['gross_sales_ty']/df['quantity_actuals']).alias("initial_average_retail_price_ty"))
    df = df.with_columns((df['gross_sales_ly']/df['sold_qty_ly']).alias("initial_average_retail_price_ly"))
    df = df.with_columns((df['gross_sales_lly']/df['quantity_ppy']).alias("initial_average_retail_price_lly"))



    # df = df.with_columns((pl.when(df['historical_year']==l_year)
    #                                 .then(df['COSTPRICE'])
    #                                 .otherwise(0)).alias('cost_of_goods_ly'))
    # df = df.with_columns((pl.when(df['historical_year']==ll_year)
    #                                 .then(df['COSTPRICE'])
    #                                 .otherwise(0)).alias('cost_of_goods_lly'))

    df = df.with_columns(((df['OpeningStock']+df['StockReceivedQty'])*df["initial_average_retail_price"]).alias("purchase_value"))
    df = df.with_columns(df['sales_actual'].alias("sales_act_forecast"))
    sums = df["purchase_value"].sum()
    df = df.with_columns(((df["purchase_value"]/sums)*100).alias('purchase_value_mix_percent'))
    sums = df['BudgetAmount'].sum()
    df = df.with_columns(((df['BudgetAmount']/sums)*100).alias("budget_percent"))
    df = df.with_columns(df["budget_percent"].alias("relative_budget_percent"))

    subset = ['ITEMID','Channel',"INVENTLOCATIONID","Budget_date"]

    df = df.with_columns((pl.col("BudgetAmount")/pl.col("BudgetAmount").count().over(subset)).alias("BudgetAmount"))
    df = df.with_columns((pl.col("BudgetCost")/pl.col("BudgetCost").count().over(subset)).alias("BudgetCost"))
    df = df.with_columns((pl.col("OpeningStock")/pl.col("OpeningStock").count().over(subset)).alias("OpeningStock"))
    df = df.with_columns((pl.col("BudgetCost")/pl.col("BudgetCost").count().over(subset)).alias("BudgetCost"))
    
    
    
    # df = df.with_columns(df["OpeningStock"].alias("StockOnHandQty"))
    # df = df.with_columns(df["OpeningStock"].alias("ClosingStock"))
    # df = df.with_columns(df["OpeningStock"].alias("TotalPurchaseQty"))
# CurrentStockCost 
# "BDate", "Budget_date", ,"UnitsBuyBySku", "units_buy_by_sku",
    df = df.rename(dict(zip(
        ["BudgetAmount","BudgetCost", "OpeningStock","ClosingStock","StockReceivedQty","INVOICEDATE"],
                            ["budget_amount","budget_cost", "opening_stock", "closing_stock", 'stock_received_qty',"History_date"])))

    # df = df.with_columns(pl.col("budget_gross_margin_percent").alias("adjusted_budget_gross_margin_percent"))
    df = df.with_columns(pl.col("adjusted_budget_gross_margin_percent").alias("budget_gross_margin_percent"))
#----------------------------------------------------------AVAILABE STOCK CALCULATION--------------------------------------------------------------
    print(df['stock_date'])
    fill_date = df['stock_date'].dt.max()
    print(fill_date, type(fill_date), 'filling_date')
    df = df.with_columns(stock_date = pl.col('stock_date').fill_null(fill_date))
    forecast_date = filters.forecast_date_range.fro
    forecast_date_as_datetime = datetime.strptime(forecast_date, '%Y-%m-%d')
    forecast_date_as_date = datetime.date(forecast_date_as_datetime)
    #-------------------------------------------------------------------
    if date.today() < forecast_date_as_date:
        print('budget_stock')
        df = df.with_columns((pl.when(df['budget_date'].cast(pl.Date)>date.today(), df['budget_date'].cast(pl.Date)<forecast_date_as_date)
                              .then(df['budget_qty'])
                              .otherwise(0)).alias('budget_qty_to_subtract'))
        df = df.with_columns(stock_on_hand_qty = pl.col('closing_stock')-pl.col('to_calculate_the_stock_till_now')-pl.col('budget_qty_to_subtract'))
        df = df.with_columns((pl.when(df['History_date'].cast(pl.Date)==(forecast_date_as_date-timedelta(days = 365)))
                                      .then(df['closing_stock'])
                                      .otherwise(0)).alias('stock_on_hand_qty_ly'))
        df = df.with_columns((pl.when(df['History_date'].cast(pl.Date)==(forecast_date_as_date-timedelta(days = 730)))
                                      .then(df['closing_stock'])
                                      .otherwise(0)).alias('stock_on_hand_qty_lly'))
                            
        print(df['stock_on_hand_qty'].sum())
    if date.today() == forecast_date_as_date:
        print('sales_stock')
        df = df.with_columns(stock_on_hand_qty = pl.col('closing_stock')-pl.col('to_calculate_the_stock_till_now'))
        print(df['stock_on_hand_qty'].sum())
        df = df.with_columns((pl.when(df['History_date'].str.to_date('%Y-%m-%d', strict=False)==(date.today()-timedelta(days = 365)))
                                      .then(df['closing_stock'])
                                      .otherwise(0)).alias('stock_on_hand_qty_ly'))
        df = df.with_columns((pl.when(df['History_date'].str.to_date('%Y-%m-%d', strict=False)==(date.today()-timedelta(days = 730)))
                                      .then(df['closing_stock'])
                                      .otherwise(0)).alias('stock_on_hand_qty_lly'))
    #-------------------------------------------------------------------
#-----------------------------------------------------------------------
    return df

def inititalize_columns(df:pl.DataFrame,gloabal_vars:OTB) ->pl.DataFrame:

    for col in gloabal_vars.other_cols:
        df = df.with_columns(pl.lit(0).alias(col))
    for col in gloabal_vars.quantity_cols:
        df = df.with_columns(pl.lit(0).alias(col))
    for col in gloabal_vars.margin_cols:
        df = df.with_columns(pl.lit(0).alias(col))
    for col in gloabal_vars.cost_cols:
        df = df.with_columns(pl.lit(0).alias(col))

    for col in gloabal_vars.avg_col:
        df = df.with_columns((df[col].replace({np.inf:0,-np.inf:0}).fill_nan(0)).alias(col))
    for col in gloabal_vars.sum_col:
        df = df.with_columns((df[col].replace({np.inf:0,-np.inf:0}).fill_nan(0)).alias(col))

    return df

    



    