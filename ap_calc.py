import datetime
from collections import namedtuple
from datetime import datetime, timedelta
from time import ctime
from typing import Dict, List

import pandas as pd
import numpy as np
import polars as pl

from rapidframes import QueryBuilder, RapiDF
from routes.AP.parameters import Parameters

from warnings import simplefilter 
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
    RA_TRNX        = "ra_kpi_joined"
    SALES_TRNX     = "cst_trnx"
    filtered_symobl = "filtered_symobl"
    SALES_FILTERED = "sales_filtered"
    # AP_TRNX      =  "otb_trnx"
    RA_SALES_JOIN  = "ra_sales_join"
    KPI_TRNX    = "kpi_trnx"
    #RA_OTB_JOIN   =  "ra_otb_join"

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

def create_ra(filters):
    """ Filter out the Range Arch Trx Date on the basis of forecast date range """
    print(f"[{ctime()}] [INFO] filtering RA Transactions")
    filters_map = {"Channel":filters.sales_channel,"Family":filters.product_family,
                "SubFamily":filters.sub_families,"ITEMID":filters.sku,"Category":filters.category,
                "SubCategory":filters.sub_category,"Supplier":filters.suppliers}
    
    print(rf.get_library("RA_DATA").list_symbols(), 'all_libraries')

    lib.write("filtered_symobl",filter_symbol(
        rf = rf,
        library_name=LIBRARY_NAME, symbol_name=symbols.RA_TRNX,
        filter_set={
            "Budget_date":   {"_gte_": forecast_start_date_str, "_lte_": forecast_end_date_str}
        },return_as_pl=False
    )
    )
    q = QueryBuilder()
    filtered_ra_trx =  pl.DataFrame(rf.get_library(LIBRARY_NAME).read('filtered_symobl', query_builder=q).data)
    print(filtered_ra_trx.sample(), 'ra_tttt')

    print(filtered_ra_trx.columns,"FILTERED")
    for key,val in filters_map.items():
        if val != []:
            filtered_ra_trx: pl.DataFrame = filter_symbol(
                rf = rf,
                library_name=LIBRARY_NAME, symbol_name=symbols.filtered_symobl,
                filter_set={
                    f"{key}"         : {"_isin_": val},
                }
            )

    # Get unique Channels
    unique_channels: List[str] = list(filtered_ra_trx["channel"].unique())
    unique_channels: List[str] = [i for i in unique_channels if i != None]
    # Get Unique Item IDs
    unique_item_ids: List[str] = list(filtered_ra_trx["ITEMID"].unique())
    unique_item_ids: List[str] = [i for i in unique_item_ids if i != None]
    # Get Unique Store IDs
    unique_store_ids: List[str] = list(filtered_ra_trx["INVENTLOCATIONID"].unique())
    unique_store_ids: List[str] = [i for i in unique_store_ids if i != None]

    
    print(f"[{ctime()}] [INFO] filtering sales transactions")
    
    rf.get_library(LIBRARY_NAME).write(
        symbols.SALES_FILTERED,
        filter_symbol(
            rf = rf,
            library_name=LIBRARY_NAME, symbol_name="cst_trnx",
            filter_set={
                "INVOICEDATE"     : {"_gte_": filters.history_date_range.fro, "_lte_": filters.history_date_range.to},
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
    print(filtered_sales_trx.head(), 'fil_sales')
    # filtered_sales_trx = filtered_sales_trx.rename({'channel': 'Channel'})
    # filtered_sales_trx =pl.LazyFrame(filtered_sales_trx)
    # filtered_ra_trx =pl.LazyFrame(filtered_ra_trx)

    # print(filtered_sales_trx,"hedd")
    # print(filtered_ra_trx['otb_amount'].sum(), 'dllk;ddd')
    print(f"[{ctime()}] [INFO] Applying Joins between Filtered RA TRX and Filtered Sales TRX")
    
    rf.get_library(LIBRARY_NAME).write(
        symbols.RA_SALES_JOIN,
        filtered_ra_trx.join(
            filtered_sales_trx,
            on=["ITEMID", "channel", "INVENTLOCATIONID"],
            how="left"
        ).to_pandas()
    )
    # raslsjoin: pl.DataFrame = pl.DataFrame(rf.get_library(LIBRARY_NAME).read(symbols.RA_SALES_JOIN, query_builder=q).data)

    # print(raslsjoin['otb_amount'].sum(), 'dllk;ddd')

    # RA_SALES_JOIN = filtered_ra_trx.join(
    #         filtered_sales_trx,
    #         on=["ITEMID", "INVENTLOCATIONID"],
    #         how="left"
    #     )


    print(f"[{ctime()}] [INFO] Applying Joins between RA_OTB_JOIN and KPI_TABLE")

    # AP_TRNX: pl.DataFrame = filter_symbol(
    #     rf = rf,
    #     library_name="RA_DATA", symbol_name=symbols.AP_TRNX
    # )

    print(f"[{ctime()}] [INFO] Applying Joins between RA_SALES_JOIN and OTB_TRNX")

    # RA_OTB_JOIN = RA_SALES_JOIN.join(AP_TRNX,
    #             on=["ITEMID","INVENTLOCATIONID","channel"],
    #         how="left"
    # )
    print("joined")
    # q = QueryBuilder()
    # datas = rf.get_library("RA_DATA").read(symbols.RA_OTB_JOIN,query_builder=q).data
    # print("joined")
    # KPI_TRNX: pl.DataFrame = filter_symbol(
    #     rf = rf,
    #     library_name="RA_DATA", symbol_name=symbols.KPI_TRNX
    # )

    # print(f"[{ctime()}] [INFO] Applying Joins between RA_OTB_JOIN and KPI_TABLE")

    # KPI_TABLE_JOIN = RA_OTB_JOIN.join(KPI_TRNX,
    #             on=["ITEMID"],
    #         how="left"
    # )

    # print(f"[{ctime()}] [INFO] Applying Joins between RA_OTB_JOIN and KPI_TABLE")
    # print(history_start_date_str, history_end_date_str,"this is what we have")

    # Divide the merged table on three years
    # date_wise_columns = get_sales_year(historical_start_date=history_start_date_str, historical_stop_date=history_end_date_str)
    q = QueryBuilder()
    datas = rf.get_library(LIBRARY_NAME).read(symbols.RA_SALES_JOIN,query_builder=q).data
    # print(datas["otb_amount"].sum(),"OTBAMOUNT")
    # datas = rf.get_library(LIBRARY_NAME).read(symbols.RA_OTB_JOIN,query_builder=q).data
    # print(type(datas),"rapid frame")
    print("joined")
    return datas

#---------------------------////------||------////--------------------------------------------


def form_base_data(df:pd.DataFrame) -> pl.DataFrame:
    """
    Form base data for further analysis.

    Parameters:
        df (pd.DataFrame): Input pandas DataFrame.

    Returns:
        pl.DataFrame: Processed polars DataFrame for further analysis.
    """

    current_datetime = datetime.now()
    cur_year = str(current_datetime.year)
    l_year = str(int(cur_year)-1)
    ll_year = str(int(l_year)-1)
    df = pl.from_pandas(df)

    print(round(df.estimated_size('mb'),2)," MB memory size of data")
    print(round(df.estimated_size('gb'),2)," GB memory size of data")

    df = df.with_columns(budget_year  = pl.col('Budget_date').dt.strftime('%Y').cast(pl.Int16))

    #fill null value associated with history data
    df = df.with_columns((df['historical_year'].fill_nan(0).cast(pl.Int16).cast(pl.Utf8).fill_null("unknown")))
    df = df.with_columns((df['history_quarter'].fill_null("unknown")))
    df = df.with_columns((df['history_month'].fill_null("")))
    df = df.with_columns((df['history_week'].fill_null("unknown")))
    df = df.with_columns((df['history_day'].fill_null("unknown")))
    df = df.with_columns((df["INVOICEDATE"].cast(pl.Date).cast(pl.Utf8).fill_null("unknown")))  
    df = df.with_columns((df["Budget_date"].cast(pl.Date).cast(pl.Utf8)))  

   
    # create new columns based on historical year
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
    df = df.with_columns((pl.when(df['historical_year']==l_year)
                                    .then(df['gross_sales'])
                                    .otherwise(0)).alias('gross_sales_ly'))


    #calculate some metrics
    sums = df["net_sales_ly"].sum()
    df = df.with_columns(((df["net_sales_ly"]/sums)*100).alias('net_sales_mix_percent'))

    df = df.with_columns((df["net_sales_ly"]/df['sold_qty_ly']).alias("final_price"))
    df = df.with_columns((df['gross_sales_ly']/df['sold_qty_ly']).alias("initial_average_retail_price"))
    # df = df.with_columns(((df['OpeningStock']+df['StockReceivedQty'])*df["initial_average_retail_price"]).alias("purchase_value"))
    df = df.with_columns(df['sales_actual'].alias("sales_act_forecast"))
    # sums = df["purchase_value"].sum()
    # df = df.with_columns(((df["purchase_value"]/sums)*100).alias('purchase_value_mix_percent'))


    # sums = df['BudgetAmount'].sum()
    # df = df.with_columns(((df['BudgetAmount']/sums)*100).alias("budget_percent"))
    # df = df.with_columns(df["budget_percent"].alias("relative_budget_percent"))
    subset = ['ITEMID','channel',"INVENTLOCATIONID","Budget_date"]

    #Level the budget related columns for correcting errors
    df = df.with_columns((pl.col("otb_amount")/pl.col("otb_amount").count().over(subset)).alias("otb_amount"))

    # df = df.with_columns((df.group_by('ITEMID')['otb_amount'].transform(lambda x:(x!=0).cumsum()==1)).alias('otb_sku_count'))
    df = df.with_columns(((pl.col('ITEMID').is_first().cast(pl.UInt8)*pl.col('otb_amount')!=0)).alias('otb_sku_count'))
    print(df['otb_sku_count'], df['otb_sku_count'].sum(), 'the_otb_sku_count')
    
    df = df.with_columns((pl.when(df['historical_year'] == cur_year)
                          .then(df['otb_sku_count'])
                          .otherwise(0).alias('otb_sku_count_ty')))
    
    df = df.with_columns((pl.when(df['historical_year'] == l_year)
                          .then(df['otb_sku_count'])
                          .otherwise(0).alias('otb_sku_count_ly')))
    
    df = df.with_columns((pl.when(df['historical_year'] == ll_year)
                          .then(df['otb_sku_count'])
                          .otherwise(0).alias('otb_sku_count_lly')))
    df = df.with_columns(((pl.col('ITEMID').is_first().cast(pl.UInt8)*pl.col('budget_amount')!=0)).alias('budget_sku_count'))
    df = df.with_columns((pl.when(df['historical_year'] == cur_year)
                          .then(df['budget_sku_count'])
                          .otherwise(0).alias('budget_sku_count_ty')))
    
    df = df.with_columns((pl.when(df['historical_year'] == l_year)
                          .then(df['budget_sku_count'])
                          .otherwise(0).alias('budget_sku_count_ly')))
    
    df = df.with_columns((pl.when(df['historical_year'] == ll_year)
                          .then(df['budget_sku_count'])
                          .otherwise(0).alias('budget_sku_count_lly')))
    df = df.with_columns((pl.col("otb_cost")/pl.col("otb_cost").count().over(subset)).alias("otb_cost"))
    # df = df.with_columns((pl.col("OpeningStock")/pl.col("OpeningStock").count().over(subset)).alias("OpeningStock"))
    # df = df.with_columns((pl.col("BudgetCost")/pl.col("BudgetCost").count().over(subset)).alias("BudgetCost"))
    # df = df.with_columns(df["OpeningStock"].alias("StockOnHandQty"))
    # df = df.with_columns(df["OpeningStock"].alias("ClosingStock"))
    # df = df.with_columns(df["OpeningStock"].alias("TotalPurchaseQty"))
    df = df.with_columns(unit_buy_by_sku = pl.col('budget_qty'))
    df = df.with_columns((pl.when(df['budget_year']==l_year)
                                    .then(df['budget_qty'])
                                    .otherwise(0)).alias('units_buy_by_sku_ly'))
    
    df = df.with_columns((pl.when(df['budget_year']==ll_year)
                                    .then(df['budget_qty'])
                                    .otherwise(0)).alias('units_buy_by_sku_lly'))
    
    df = df.with_columns((pl.when(df['budget_year']==l_year)
                                    .then(df['budget_amount'])
                                    .otherwise(0)).alias('budget_amount_ly'))
    
    df = df.with_columns((pl.when(df['budget_year']==l_year)
                                    .then(df['budget_cost'])
                                    .otherwise(0)).alias('budget_cost_ly'))
    df = df.with_columns((pl.when(df['budget_year']==ll_year)
                                    .then(df['budget_cost'])
                                    .otherwise(0)).alias('budget_cost_lly'))


    df = df.with_columns((pl.when(df['budget_year']==ll_year)
                                    .then(df['budget_amount'])
                                    .otherwise(0)).alias('budget_amount_lly'))
# KM's GRAPH cols---
    df = df.with_columns((pl.when(df['budget_year']==l_year)
                                    .then(df['otb_amount'])
                                    .otherwise(0)).alias('otb_amount_ly'))
    
    df = df.with_columns((pl.when(df['budget_year']==ll_year)
                                    .then(df['otb_amount'])
                                    .otherwise(0)).alias('otb_amount_lly'))

    df = df.with_columns((pl.when(df['budget_year']==l_year)
                                    .then(df['otb_qty'])
                                    .otherwise(0)).alias('otb_qty_ly'))

    df = df.with_columns((pl.when(df['budget_year']==ll_year)
                                    .then(df['otb_qty'])
                                    .otherwise(0)).alias('otb_qty_lly'))
    
    df = df.with_columns((pl.when(df['budget_year']==ll_year)
                                    .then(df['otb_qty'])
                                    .otherwise(0)).alias('otb_qty_lly'))
    
    # df = df.with_columns(())
    
    # Rename columns for consistency
    # df = df.rename(dict(zip(
    #     ["StockOnHandQty","OpeningStock","ClosingStock","TotalPurchaseQty","UnitsBuyBySku","BudgetGrossMargin%",'CurrentStockCost',"StockReceivedQty","INVOICEDATE","Budget_date","BudgetYear","channel"],
    #                         ["stock_on_hand_qty","opening_stock","closing_stock","total_purchase_qty","units_buy_by_sku","budget_gross_margin_percent",'current_stock_cost_at_retail','stock_received_qty',"History_date","Budget_date","Budget_Year","Channel"])))
    # df = df.rename(dict(zip(
    #     ["budget_amount","budget_cost", "PurchaseRetailValueatGrossSale", "OTBorPurchaseCost","StockOnHandQty","OpeningStock","ClosingStock","TotalPurchaseQty","UnitsBuyBySku","budget_gross_margin_percent",'CurrentStockCost','budget_qty',"StockReceivedQty","channel","INVOICEDATE","Budget_date","Budget_Year"],
    #                         ["budget_amount","budget_cost", "otb_amount", "otb_cost","stock_on_hand_qty","opening_stock","closing_stock","total_purchase_qty","units_buy_by_sku","budget_gross_margin_percent",'current_stock_cost_at_retail','budget_qty','stock_received_qty',"Channel","History_date","Budget_date","Budget_Year"])))
    # "Budget_date",
    # "Budget_date",
    df = df.rename(dict(zip(
        ["channel", "budget_year", 'INVOICEDATE'],
                ["Channel","Budget_Year", 'History_date']
    )))
    # df = df.with_columns(pl.col("budget_gross_margin_percent").alias("adjusted_budget_gross_margin_percent"))

    return df


def inititalize_columns(df:pl.DataFrame,gloabal_vars:Parameters) ->pl.DataFrame:
    """
    Initialize new column with 0 as value.

    Args:
        df (pl.DataFrame): Input DataFrame.
        gloabal_vars (Parameters): Global variables containing column names.

    Returns:
        pl.DataFrame: DataFrame with initialized columns.

    """
    print("it enters here as")
    for col in gloabal_vars.other_cols:
        df = df.with_columns(pl.lit(0).alias(col))
    for col in gloabal_vars.quantity_cols:
        df = df.with_columns(pl.lit(0).alias(col))
    for col in gloabal_vars.margin_cols:
        df = df.with_columns(pl.lit(0).alias(col))
    for col in gloabal_vars.cost_cols:
        df = df.with_columns(pl.lit(0).alias(col))
    df = df.with_columns(Check_box = pl.lit(1))
    df = df.with_columns(budget_vpy = pl.lit(0))
    df = df.with_columns(budget_vppy = pl.lit(0))
    df = df.with_columns(StockatRetailPrice = pl.lit(0))
    df = df.with_columns(stock_on_hand_qty = pl.lit(0))
    df=df.with_columns(OpeningStock=pl.lit(0))
    df = df.with_columns(stock_cost_ly = pl.lit(0))
    print(df.columns,"DFCOLUMNS")
    sums = df['otb_amount'].fill_null(0).sum()
    df = df.with_columns(((df['otb_amount']/sums)*100).alias("otb_percent"))
    print(df["otb_percent"].sum(), "otb percent")
    df = df.with_columns((df["otb_percent"].fill_null(0)).alias("relative_otb_percent"))
    print(df["relative_otb_percent"].sum(), "relatve otb percent")
    # for col in Parameters.max_col:


    #     if col != "check_kpi":


    #         df = df.with_columns((pl.col(col).fill_null("unknown")).alias(col))

    return df

    



    