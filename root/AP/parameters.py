from pydantic import BaseModel
from typing import List,Dict
from sqlalchemy.orm import Session
from sqlalchemy import text,inspect
import pandas as pd
import numpy as np
import datetime
import calendar
import os
from decimal import Decimal
from celery.result import AsyncResult
import subprocess
import time
import psycopg2
import math
import sys
import polars as pl
pd.options.mode.chained_assignment = None 


from typing import List,Dict,Optional,Union
from fastapi import Depends
from time import ctime
from rapidframes import RapiDF
from .schemas import  Filters, Echelons
import traceback


class Parameters:
  

    columns = ["Check_box","check_kpi","Channel",'country',"Region",'area','city',"INVENTLOCATIONID",
                "Store",'season',"Department","Family","SubFamily","Supplier","Category","DOM_COMM",
                "SubCategory","ExtendedSubCategory","SubCategorySupplier",
                "AssemblyCodeNickName","Status","ENDOFLife","Description","ITEMID","Budget_Year",
                'budget_quarter','budget_month','budget_week','budget_day','Budget_date',"historical_year",'history_quarter',
                'history_month','history_week',"history_day","History_date",
                "otb_amount", "otb_amount_ly", "otb_amount_lly", "otb_percent", 'revised_otb_amount','new_otb_mix', 'renewed_otb_percent', 
                'otb_sku_count', 'otb_sku_count_ty', 'otb_sku_count_ly', 'otb_sku_count_lly', 
                'budget_sku_count', 'budget_sku_count_ty', 'budget_sku_count_ly', 'budget_sku_count_lly',
                're_assigned_mix',
                "deficit", 'relative_otb_percent', 'sales_act_vs_forecast',
                "otb_vs_act_forecast_percent","otb_vs_py_percent","otb_vs_ppy_percent",
                "initial_average_retail_price","units_buy_by_sku","total_sku_count", "total_sku_count_ly", 'total_sku_count_lly', "otb_vs_sku",
                "otb_vs_sku_ly",
                "otb_vs_act_forecast_percent_ly", "sales_actual",
                "net_sales_ly","net_sales_lly",
                "act_forecast_vs_ly_percent", "budget_amount", "budget_cost", "budget_qty", "budget_amount_ly", 'budget_amount_lly',
                "StockatRetailPrice", "stock_on_hand_qty", "sold_article_count_ly", 'sold_article_count_lly', 
                'stock_qty_actual', 'stock_qty_ly', 'stock_qty_lly',
                'stock_retail_actual', 'stock_retail_ly', 'stock_retail_lly', 'stock_cost_actual', 'stock_cost_ly', 'stock_cost_lly', 
                'article_score_sale', 'article_score_abc', 'article_score_ae', 'article_score_speed', 'article_score_terminal', 'article_score_margin', 
                'article_score_sell', 'article_score_markdown', 'article_score_core', 
                'article_score_quartile', 'article_score_sortimeter', 'coefficient_score', 'coefficient_score_mix_percent'] 

    other_cols = ["cost_act_forecast_vs_ly_percent", "relative_otb_percent", "margin_act_forecast_vs_ly_percent", "quantity_act_forecast_vs_ppy_percent", "margin_otb_vs_sku", "lly_margin_percent", 
    "otb_vs_sku_qty", "otb_percent","sales_act_vs_forecast","Check_box"]    

    

    # max_col = ["Channel","Family","SubFamily","Supplier","Category","DOM_COMM","SubCategory","ExtendedSubCategory",
    #              "SubCategorySupplier","AssemblyCodeNickName",
    #             "Status","ENDOFLife","Check_box","Description", "ITEMID", "Store",
    #         'history_month','history_week',"History_date",'history_quarter',"history_day","budget_quarter","Budget_Year","historical_year",
    #             'budget_month','budget_week','budget_day','Budget_date','INVENTLOCATIONID','Department','area',"Region"] 

    max_col =['Channel','Family', 'SubFamily', 'Supplier','Category', 'DOM_COMM',
                       'SubCategory', 'ExtendedSubCategory', 'SubCategorySupplier', 'AssemblyCodeNickName', 
                        'Status', 'ENDOFLife','Check_box','Description', 'ITEMID','Region','area','INVENTLOCATIONID','Store',"Department",'Budget_Year',
                        'budget_quarter','budget_month','budget_week','budget_day','Budget_date','historical_year','history_quarter','history_month',
                        'history_week','history_day','History_date']          
                
    sum_col =  ["otb_amount", "otb_amount_ly", "otb_amount_lly",
                'otb_qty', 'otb_qty_ly', 'otb_qty_lly',
                'otb_sku_count', 'otb_sku_count_ty', 'otb_sku_count_ly', 'otb_sku_count_lly',
                "otb_cost","otb_percent",'sales_act_vs_forecast',"budget_amount_ly", 'budget_amount_lly',
                'budget_sku_count', 'budget_sku_count_ty', 'budget_sku_count_ly', 'budget_sku_count_lly', 
                "units_buy_by_sku","total_sku_count", 'relative_otb_percent',
                "net_sales_ly","net_sales_lly","sold_article_count_ly", 'sold_article_count_lly',
               "sales_actual", "StockatRetailPrice", "stock_on_hand_qty", "units_buy_by_sku_ly", 'units_buy_by_sku_lly',
               "total_sku_count_ly", 'total_sku_count_lly',
                "quantity_actuals","quantity_ppy","quantity_act_forecast",
                "quantity_act_forecast _vs_sku", 
                "cost_actuals","otb_cost_percent", "cost_mix_percent","cost_act_forecast _vs_sku","cost_act_forecast",
                "cost_otb_vs_sku", "sold_qty_ly",
                "margin_actuals","margin_act_forecast_vs_sku","margin_mix_percent","margin_act_forecast", "otb_vs_ppy_margin_percent"
                ,"otb_margin_mix_percent","cost_of_goods_ly","cost_of_goods_lly", "budget_amount", "budget_cost", "budget_qty",
                'budget_cost_lly', 'budget_cost_ly', 'stock_qty_actual', 'stock_qty_ly', 'stock_qty_lly', 
                'stock_retail_actual', 'stock_retail_ly', 'stock_retail_lly',
                'stock_cost_actual', 'stock_cost_ly', 'stock_cost_lly'
                ]    #"Otb_qty_percent","Cost/SKU",

    
    avg_col = ["deficit","otb_vs_sku","otb_vs_act_forecast_percent",
                "otb_vs_py_percent","otb_vs_ppy_percent",
                "quantity_act_forecast_vs_ppy_percent","otb_vs_qty_ppy_percent",
                "otb_vs_sku_qty", "quantity_act_forecast_vs_otb_percent",
                "otb_vs_act_forecast_cost_percent","otb_vs_py_cost_percent","otb_vs_ppy_cost_percent","cost_act_forecast_vs_ly_percent",
                "cost_act_forecast_vs_ppy_percent","otb_gross_margin", "act_forecast_vs_ly_percent",
                
                "otb_gross_margin_percent","margin_act_forecast_vs_ppy_percent","margin_act_forecast_vs_ly_percent", "budget_vpy", "budget_vppy",
                "margin_otb_vs_sku","otb_vs_py_margin_percent",'initial_average_retail_price',"ly_margin_percent","lly_margin_percent"
                ]        #"Otb vs Act/Forecast QTY%","cost_act_forecast _vs_otb%",

    
    int_cols = ["sales_actual","units_buy_by_sku","total_sku_count"]

    float_cols = ["otb_amount", "deficit",'otb_percent', 'relative_otb_percent','sales_act_vs_forecast',"otb_vs_act_forecast_percent",
                "otb_vs_py_percent","otb_vs_ppy_percent","initial_average_retail_price","cost_act_forecast_vs_ly_percent","cost_act_forecast","cost_of_goods_ly"]   


    heirarchy = ["Channel","Family","SubFamily","Supplier","Category","DOM_COMM","SubCategory","ExtendedSubCategory","SubCategorySupplier"
            ,"AssemblyCodeNickName","Status","ENDOFLife","Description","ITEMID"]

    editable_cols = ["otb_amount","otb_percent","otb_vs_sku","otb_vs_py_percent","otb_vs_ppy_percent","units_buy_by_sku"
                    ,"otb_vs_act_forecast_percent"] 

    rank_col = ["Check_box","order_index"]
    kpi_metrics = ['article_score_sale','article_score_abc','article_score_ae','article_score_speed','article_score_terminal',
                    'article_score_margin','article_score_sell','article_score_markdown','article_score_core',
                    'article_score_quartile','article_score_sortimeter']    
    article_score =['sale','abc','ae','speed','terminal','margin','sell','markdown','core','quartile','sortimeter']

    kpi_col = ["check_kpi"]

    percent_col = ['otb_percent', 'relative_otb_percent',"otb_vs_act_forecast_percent", "otb_vs_py_percent","otb_vs_ppy_percent", "otb_vs_qty_ppy_percent", "quantity_act_forecast_vs_otb_percent",
                    "quantity_act_forecast_vs_ppy_percent", "otb_vs_py_cost_percent","otb_vs_ppy_cost_percent", "otb_cost_percent", "otb_vs_act_forecast_cost_percent","otb_vs_py_cost_percent",
                    "cost_mix_percent", "cost_act_forecast_vs_ly_percent", "cost_act_forecast_vs_ppy_percent", "otb_gross_margin_percent","otb_margin_mix_percent",
                    "otb_vs_act_forecast_margin_percent","otb_vs_py_margin_percent","otb_vs_ppy_margin_percent","margin_mix_percent","margin_act_forecast_vs_ly_percent","margin_act_forecast_vs_ppy_percent",
                    'coefficient_score_mix_percent', 'new_otb_mix']  
    

    quantity_cols = ["otb_vs_qty_ppy_percent", "units_buy_by_sku",'units_buy_by_sku_ly', 'units_buy_by_sku_lly',
                    "otb_vs_sku_qty", 'otb_qty', 'otb_qty_ly', 'otb_qty_lly',
                    "quantity_act_forecast","quantity_act_forecast_vs_otb_percent",
                    "quantity_act_forecast _vs_sku","quantity_act_forecast_vs_ppy_percent"]         
    
    ra_quantity = ["Channel",'country',"Region",'area','city',"INVENTLOCATIONID",
                    "Store",'season',"Department","Family","SubFamily","Supplier","Category","DOM_COMM","SubCategory",
                    "ExtendedSubCategory","SubCategorySupplier","AssemblyCodeNickName","Status","ENDOFLife",
                    "Description","ITEMID",
                    "units_buy_by_sku", 'units_buy_by_sku_ly', 'units_buy_by_sku_lly',
                    "otb_vs_qty_ppy_percent",
                    'otb_qty', 'otb_qty_ly', 'otb_qty_lly',
                    "initial_average_retail_price","Units Buy/SKU","total_sku_count","otb_vs_sku_qty","quantity_actuals",
                    "quantity_act_forecast","quantity_act_forecast_vs_otb_percent",
                    "quantity_act_forecast _vs_sku","quantity_ppy","quantity_act_forecast_vs_ppy_percent"]      
                    

    cost_cols = ["otb_cost_percent","otb_vs_act_forecast_cost_percent","otb_vs_py_cost_percent","otb_vs_ppy_cost_percent",
                    "cost_mix_percent",
                    "cost_act_forecast","cost_act_forecast_vs_ly_percent",
                    "cost_act_forecast _vs_sku","cost_act_forecast_vs_ppy_percent",
                    "cost_otb_vs_sku",
                    'budget_cost_ly', 'budget_cost_lly']                  

                  


    ra_cost = ["Channel",'country',"Region",'area','city',"INVENTLOCATIONID",
                "Store",'season',"Department","Family","SubFamily","Supplier","Category","DOM_COMM","SubCategory",
                "ExtendedSubCategory","SubCategorySupplier","AssemblyCodeNickName","Status","ENDOFLife",
                "Description","ITEMID",
                "otb_cost","otb_cost_percent","otb_vs_act_forecast_cost_percent","otb_vs_py_cost_percent","otb_vs_ppy_cost_percent",
                "initial_average_retail_price","total_sku_count","cost_otb_vs_sku","cost_mix_percent","cost_actuals",
                "cost_act_forecast","cost_act_forecast_vs_ly_percent",
                "cost_act_forecast _vs_sku","cost_of_goods_lly", 'budget_cost_ly', 'budget_cost_lly']               


    margin_cols = ["otb_gross_margin","margin_actuals","otb_gross_margin_percent","otb_margin_mix_percent","margin_act_forecast",
                    "otb_vs_act_forecast_margin_percent","otb_vs_py_margin_percent","otb_vs_ppy_margin_percent",
                    
                    "buy_margin/sku","margin_otb_vs_sku","margin_mix_percent",
                    "margin_act_forecast","margin_act_forecast_vs_ly_percent",
                    "margin_act_forecast_vs_sku","margin_act_forecast_vs_ppy_percent","ly_margin_percent"]

                
                

    ra_margin = ["Channel",'country',"Region",'area','city',"INVENTLOCATIONID",
                "Store",'season',"Department","Family","SubFamily","Supplier","Category","DOM_COMM","SubCategory",
                "ExtendedSubCategory","SubCategorySupplier","AssemblyCodeNickName","Status","ENDOFLife",
                "Description","ITEMID","otb_gross_margin",
                "otb_gross_margin_percent","otb_margin_mix_percent","margin_act_forecast","otb_vs_act_forecast_margin_percent","otb_vs_py_margin_percent","otb_vs_ppy_margin_percent",
                "initial_average_retail_price","buy_margin/sku","total_sku_count","margin_otb_vs_sku","margin_mix_percent","margin_actuals","ly_margin_percent","lly_margin_percent",
                "margin_act_forecast_vs_ly_percent",
                "margin_act_forecast_vs_sku","margin_act_forecast_vs_ppy_percent"]

    SCORES: dict  = {'sale':'article_score_sale','abc':'article_score_abc','ae':'article_score_ae',
    'speed':'article_score_speed','terminal':'article_score_terminal','margin':'article_score_margin',
     'sell':'article_score_sell','markdown':'article_score_markdown','core':'article_score_core','quartile':'article_score_quartile','sortimeter':'article_score_sortimeter'}

    filter_details = {
                    "sales_channel":"Channel",
                    "product_family":"Family",
                    "sub_families":"SubFamily",
                    "category":"Category",
                    "sub_category":"SubCategory",
                    "suppliers":"Supplier",
                    "sku":"ITEMID"
                    }

    tabs = {
            "BudgetValue":columns,
            "BudgetCost":ra_cost,
            "BudgetQuantity":ra_quantity,
            "BudgetMargin":ra_margin 
            }

    SUB_FILTER = {'store':[],'region':[],'season':[],"area":[],'Channel':[],'budget_year':[],'Quarter':[],'month':[],'week':[],'date':[],'Day':[],
                    'history_year':[],'history_Quarter':[],'history_month':[],'history_week':[],'history_Day':[],'history_dates':[], 'article_scores' : []}
    filter_store = dict(zip(['Store','Region','Channel','Budget_Year','budget_quarter','budget_month','budget_week','budget_day','Budget_date','historical_year','history_quarter'
            ,'history_month','history_week','history_day',"History_date"],[[]]*15))

    group_subset = {"ITEMID","Channel",'Store','Region','Channel','budget_Year','budget_quarter','budget_month','budget_week','budget_day','budget_date','historical_year','history_quarter'
            ,'history_month','history_week','history_day',"History_date"}
    

    hier_ap = ['Channel', 'Family', 'SubFamily', 'Supplier', 'Category','DOM_COMM','SubCategory']



gloabal_vars = Parameters()




class Otb():
    columns = gloabal_vars.columns
    max_col = gloabal_vars.max_col
    sum_col = gloabal_vars.sum_col 
    avg_col = gloabal_vars.avg_col
    int_cols = gloabal_vars.int_cols
    float_cols = gloabal_vars.float_cols
    rank_col = gloabal_vars.rank_col
    kpi_col = gloabal_vars.kpi_col
    heirarchy = gloabal_vars.heirarchy
    tabs = gloabal_vars.tabs
    SUB_FILTER = gloabal_vars.SUB_FILTER
    filter_store = gloabal_vars.filter_store
    
    article_score = gloabal_vars.article_score
    art = gloabal_vars.SCORES

    hier_ap = gloabal_vars.hier_ap




    DATA              = pl.DataFrame()
       
       # Calculate initial frame and apply filters
    def initial_frame_calculation(self,df:pl.DataFrame):
        
        df = df.with_columns(check_kpi = pl.lit(0))
        df = df.with_columns((pl.lit(0.0)).alias("coefficient_score"))


        df = self.calculate_initial_frame(df,['ITEMID'])
        group = []
        self.SUB_FILTER = self.call_filter(df,self.SUB_FILTER,group,df,self.filter_store)
        Channel_flag = False

        # Calculate dataframe and convert max_col to string
        self.DATA,ds,Channel_flag  = self.calculate_df(df,df,Channel_flag)
        # print(self.DATA.columns, 'columns_of_')
    
        return self.DATA,self.DATA 
 
    def calculate_initial_frame(self,df:pl.DataFrame,subset:List):

        '''
        Perform initial operations on the DataFrame before grouping.

        Args:
            df (DataFrame): The input DataFrame.
            subset (list): List of column names to be used as the subset for duplicate checking.

        Returns:
            DataFrame: The DataFrame after performing initial operations.
        '''
       
        print(type(df),"DFTYPE")
      
        
        #                   Fill NoneTypes with zero
        df = df.with_columns(pl.col(gloabal_vars.max_col).fill_null(0))
        df = df.with_columns(pl.col(list(self.art.values())).replace({np.inf:0, -np.inf:0}).fill_null(0).fill_nan(0))
        
        # print(df['Channel'].value_counts().glimpse(), 'channel counts')
        # print(df['Family'].value_counts().glimpse(), 'channel counts')
        # print(df['SubFamily'].value_counts().glimpse(), 'channel counts')
        # print(df['Supplier'].value_counts().glimpse(), 'channel counts')
        df = df.with_columns(pl.col(gloabal_vars.max_col).fill_null(0))
        df = df.with_columns(pl.col("otb_amount").replace({np.inf:0, -np.inf:0}).fill_nan(0).fill_null(0))
        df = df.with_columns(pl.col("otb_percent").replace({np.inf:0, -np.inf:0}).fill_nan(0).fill_null(0))

        df = df.with_columns((pl.col("otb_amount") - (pl.col("units_buy_by_sku") * pl.col("initial_average_retail_price"))).fill_nan(0).alias("deficit"))
        print(len(df),"LENGTH")
        df= df.with_columns((pl.struct(subset).is_first_distinct().cast(pl.Int8).alias("total_sku_count")))
        df= df.with_columns((pl.col("sold_qty_ly").sum().over(subset) * pl.col("total_sku_count")).gt(0).cast(pl.Int8).alias("sold_article_count_ly"))
        df= df.with_columns((pl.col("quantity_ppy").sum().over(subset) * pl.col("total_sku_count")).gt(0).cast(pl.Int8).alias("sold_article_count_lly"))

        df= df.with_columns((pl.col("units_buy_by_sku_ly").sum().over(subset) * pl.col("total_sku_count")).gt(0).cast(pl.Int8).alias("total_sku_count_ly"))
        df= df.with_columns((pl.col("units_buy_by_sku_lly").sum().over(subset) * pl.col("total_sku_count")).gt(0).cast(pl.Int8).alias("total_sku_count_lly"))
        
        # print(df.columns,"DFCOLUMNS")

        return df
    

    
    def call_filter(self,data:pl.DataFrame,SUB_FILTER:dict,group:list,DATA:pl.DataFrame,filter_store:Dict) ->Dict:


        '''
        Append filters to SUB_FILTER based on the selected group and available data.

        Args:
            data (pd.DataFrame): The input DataFrame containing the data.
            SUB_FILTER (dict): Dictionary to store filter values.
            group (list): List of selected group names.
            DATA (pd.DataFrame): The DataFrame to use for filters.
            filter_store: A dictionary to store filter values based on the selected group.

        Returns:
            dict: Updated SUB_FILTER dictionary.
        '''

        try:
            keys = ['store','region','Channel','budget_year','Quarter','month','week','Day','date','history_year','history_Quarter',
            'history_month','history_week','history_Day','history_dates',
            'family', 'sub_family', 'supplier', 'category', 'dom_comm', 'sub_category', 'extended_sub_category', 'sub_category_supplier']

            values = ['Store','Region','Channel','Budget_Year','budget_quarter','budget_month','budget_week','budget_day','Budget_date','historical_year','history_quarter'
            ,'history_month','history_week','history_day',"History_date",
            'Family', 'SubFamily', 'Supplier', 'Category', 'DOM_COMM', 'SubCategory', 'ExtendedSubCategory', 'SubCategorySupplier']

            last_slection = ''
            if group !=[]:
                last_slection = group[-1]
            #iterate over keys and values
            print(group, 'group in call filter')
            print(last_slection, 'last_selection in call filter')
            for i,v in zip(keys,values):
                print('i is', i,'and v is', v, end =' ')
                if v not in group:
                    print('not in group')
                    if v in ['Budget_Year']:
                        SUB_FILTER[i]   = sorted(list(data[v].cast(pl.Float32).unique()),key=lambda x:(x is None,x))
                        print(SUB_FILTER[i])
                    elif v in ['Budget_date',"History_date"]:
                        SUB_FILTER[i]   = sorted(list(data[v].cast(pl.Utf8).unique()),key=lambda x:(x is None,x))
                        print(SUB_FILTER[i])
                    elif v in ['budget_month','history_month']:
                        SUB_FILTER[i]   = sorted(list(data[v].cast(pl.Utf8).unique()),key=lambda x:(x is None,list(calendar.month_name).index(x.strip()) if not pd.isna(x) else 0))
                        print(SUB_FILTER[i])
                    else:
                        SUB_FILTER[i]   = sorted(list(data[v].unique()),key=lambda x:(x is None,x)) # Channel heirarchy
                        print(SUB_FILTER[i])
                    
                    filter_store[v] = SUB_FILTER[i]
                    
                else:       #NAME IN DATA
                    print('in group')
                    print('when ', end = ' ')
                    if v in ['Budget_Year'] and v!= last_slection:
                        if group == []:
                            print('group empty')
                            SUB_FILTER[i]        = sorted(list(DATA[v].cast(pl.Float32).unique()),key=lambda x:(x is None,x))
                            print(SUB_FILTER[i])
                        else:
                            print('group not empty')
                            SUB_FILTER[i] = filter_store[v]
                            print(SUB_FILTER[i])
                    elif v in ['Budget_date',"History_date"] and v != last_slection:
                        if group == []:
                            print('group empty')
                            SUB_FILTER[i]   = sorted(list(DATA[v].cast(pl.Utf8).unique()),key=lambda x:(x is None,x))
                            print(SUB_FILTER[i])
                        else:
                            print('group not empty')
                            SUB_FILTER[i] = filter_store[v]
                            print(SUB_FILTER[i])

                    elif v in ['budget_month','history_month'] and v != last_slection:
                        if group == []:
                            print('group empty')
                            SUB_FILTER[i] = sorted(list(DATA[v].cast(pl.Utf8).unique()),key=lambda x:(x is None,list(calendar.month_name).index(x.strip()) if not pl.isna(x) else 0))
                            print(SUB_FILTER[i])

                        else:
                            print('group not empty')
                            SUB_FILTER[i] = filter_store[v]
                            print(SUB_FILTER[i])

                    else:
                        print('we are in last else have group')
                        if v!=last_slection:
                            print('v is not last selection', last_slection, v)
                            if group == []:
                                print('group empty')
                                SUB_FILTER[i]        = sorted(list(DATA[v].unique()),key=lambda x:(x is None,x))
                                print(SUB_FILTER[i])
                            else:
                                print('group not empty')
                                SUB_FILTER[i] = filter_store[v]
                                print(SUB_FILTER[i])
                
                            
            SUB_FILTER['article_score'] =  gloabal_vars.kpi_metrics  


        except Exception as e:
            print(f"error is {e}")
            print(traceback.format_exc())
            pass
        return SUB_FILTER


    def remove_special_values(self,filter_array:List) ->List:
        '''
        Remove nan/inf/-inf values from the input list.

        Args:
            filter_array (List): Input list containing values.

        Returns:
            List: List with special values removed.

        '''
        val_arr = []
        for val in filter_array:
            if val not in [float('inf'),float('-inf'),None]:
                if not isinstance(val,str):
                    if not math.isnan(val):
                        val_arr.append(val)
                else:
                    if str(val) not in ["nan","None"]:
                        val_arr.append(val)

        return val_arr


    
    def calculate_df(self, data: pl.DataFrame,ds:pl.DataFrame,Channel_flag):
        '''
        Calculation on the Dataframe
            Args:
                df : DataFrame to be calculated
            Returns:
                data: Calculated data frame

        '''
        

        Channel_flag = False
       
        data = data.with_columns((((pl.col('otb_amount')*100)/pl.col('otb_amount').sum()).replace({np.inf:0,-np.inf:0}).fill_nan(0).fill_null(0)).alias("otb_percent"))
        data = data.with_columns(pl.col("initial_average_retail_price").alias("initial_average_retail_price"))
        data = data.with_columns((pl.col("otb_amount") + pl.col("sales_actual")).alias("sales_act_vs_forecast"))
        data = data.with_columns((((pl.col("otb_amount")*100) / pl.col("sales_act_vs_forecast").replace(0,np.nan)).fill_null(0)).fill_nan(0).alias("otb_vs_act_forecast_percent"))
        data = data.with_columns(((pl.col("otb_amount")*100)/pl.col("net_sales_ly").replace(0,np.nan)).replace({np.inf:0,-np.inf:0}).fill_nan(0).fill_null(0).alias("otb_vs_py_percent"))
        data = data.with_columns(((pl.col("otb_amount")*100)/pl.col("net_sales_lly")).replace({np.inf:0,-np.inf:0}).fill_nan(0).fill_null(0).alias("otb_vs_ppy_percent"))
        data = data.with_columns(pl.col("initial_average_retail_price").replace({np.inf:0,-np.inf:0}).fill_nan(0).fill_null(0))
        data = data.with_columns(((pl.col("otb_amount") * 100) / pl.col("initial_average_retail_price").replace(0,np.nan)).fill_null(0).alias("units_buy_by_sku"))
        data = data.with_columns((((pl.col("otb_amount") / pl.col("total_sku_count")).replace({np.inf:0.0, -np.inf:0.0})).fill_null(0).fill_nan(0)).alias("otb_vs_sku"))
        data = data.with_columns(net_sales_ly = pl.col("net_sales_ly").fill_null(0))
        data = data.with_columns(net_sales_lly = pl.col("net_sales_lly"))
        data = data.with_columns((((pl.col("sales_act_vs_forecast")*100)/pl.col("net_sales_ly")).replace({np.inf:0,-np.inf:0}).fill_nan(0).fill_null(0)).alias("act_forecast_vs_ly_percent"))
        data = data.with_columns(((pl.col('stock_qty_actual')*(pl.col('sales_actual')/pl.col('quantity_actuals'))).replace({np.inf:0,-np.inf:0}).fill_nan(0).fill_null(0)).alias('stock_retail_actual'))

        data = data.with_columns(((pl.col('stock_qty_ly')*(pl.col('net_sales_ly')/pl.col('sold_qty_ly'))).replace({np.inf:0,-np.inf:0}).fill_nan(0).fill_null(0)).alias('stock_retail_ly'))
        data = data.with_columns(((pl.col('stock_qty_lly')*(pl.col('net_sales_lly')/pl.col('quantity_ppy'))).replace({np.inf:0,-np.inf:0}).fill_nan(0).fill_null(0)).alias('stock_retail_lly'))
        
        data = data.with_columns(((pl.col('cost_actuals')/pl.col('quantity_actuals')).replace({np.inf:0,-np.inf:0}).fill_nan(0).fill_null(0)).alias('unit_cost_actual'))
        data = data.with_columns(((pl.col('unit_cost_actual')*(pl.col('stock_retail_actual')/pl.col('quantity_ppy'))).replace({np.inf:0,-np.inf:0}).fill_nan(0).fill_null(0)).alias('stock_cost_actual'))
        
        data = data.with_columns(((pl.col('cost_of_goods_ly')/pl.col('sold_qty_ly')).replace({np.inf:0,-np.inf:0}).fill_nan(0).fill_null(0)).alias('unit_cost_ly'))
        data = data.with_columns(((pl.col('unit_cost_ly')*(pl.col('stock_retail_ly')/pl.col('quantity_ppy'))).replace({np.inf:0,-np.inf:0}).fill_nan(0).fill_null(0)).alias('stock_cost_ly'))

        data = data.with_columns(((pl.col('cost_of_goods_lly')/pl.col('quantity_ppy')).replace({np.inf:0,-np.inf:0}).fill_nan(0).fill_null(0)).alias('unit_cost_lly'))
        data = data.with_columns(((pl.col('unit_cost_lly')*(pl.col('stock_retail_lly')/pl.col('quantity_ppy'))).replace({np.inf:0,-np.inf:0}).fill_nan(0).fill_null(0)).alias('stock_cost_lly'))

        return data,ds,Channel_flag 
    
  


    
    def change_percent(self,grouped_df:pl.DataFrame,other_grouped_df:pl.DataFrame,increase:float,colID:str):
        """
        This function applies a percent change to a specified columns in two dataframe based on their sum
        Args:
            grouped df:The first dataframe containing the data to be adjusted
            other_grouped_df:The other dataframe containing data to br adjusted differently
            increase:The percentage change to apply
            colID:Name of column to modify in both dataframe

        """
        summation = grouped_df[colID].fill_nan(0).sum()
        grouped_df = grouped_df.with_columns((grouped_df[colID] + (grouped_df[colID]*increase)/summation).alias(colID))
        unsummation = other_grouped_df[colID].fill_nan(0).sum()
        other_grouped_df = other_grouped_df.with_columns((other_grouped_df[colID] - (other_grouped_df[colID] * increase) / unsummation).alias(colID))
        frames =  [grouped_df,other_grouped_df]
        df = pl.concat(frames)

        return df


    
    def change_value(self,grouped_df:pl.DataFrame,other_grouped_df:pl.DataFrame,increase:float,colID:str):
        print(5)

        summation = grouped_df[colID].sum()
        summation = grouped_df[colID].replace(0, pl.NaN())
        grouped_df = grouped_df.with_columns((grouped_df[colID] + (grouped_df[colID]*increase)/summation).alias(colID))
        frames =  [grouped_df,other_grouped_df]
        df = pl.concat(frames)
        return df

    
    def drill_down(self,df:pl.DataFrame,columnID:str,newValue:float):
        """
        This function distribute the value to other dataframe
        Args:
        df:The filtered dataframe in which distribution of value
        columnID:the column name of the distribution of value occur
        newValue:The amount which want to add to the columnID
        """
        print(6)
        division = newValue/len(df[columnID]) 
        df = df.with_columns((pl.col(columnID) + division).alias(columnID))
        return df[columnID]
        

    


    def drill_down_otb(self,df,columnID,newValue):
        df = df.with_columns(df[columnID]+(((df[columnID]/df[columnID].sum())*newValue)).alias(columnID))
        return df 

    
   
    def apply_secondary_filters(self,data: pl.DataFrame,filter_data: Filters,sub_filter_state:bool,
                group:List,filter_condition:[Optional]=None) ->Union[None,pl.Series,bool,List]:
        """
        
        """
        ''' We only get filters details from json response '''
        
        print('We are applying_secondary_filter')
      
        key_map = {'Store_Name':'Store','region':'Region','Channel':'Channel','BudgetYear':'Budget_Year','Quarter':'budget_quarter',
        'month':'budget_month','week':'budget_week','Day':'budget_day','BudgetDate':'Budget_date','HistoricalYear':'historical_year','history_Quarter':'history_quarter',
        'history_month':'history_month','history_week':'history_week','history_Day':'history_day','history_dates':"History_date", 'article_score':gloabal_vars.SCORES,
        # hierarchial filter conditioning
        }

    
        try:
            for key,values in filter_data.secondary_filter.dict().items():
                
                print(f'key is {key} & value is {values}')

                if not values or key =='article_score':

                    continue
                
                sub_filter_state = True
                
                if key_map[key] in ['Budget_Year']:
                    print(f"is {key_map[key]}")
                    values = [int(i) for i in values]
                    print(values, 'values in budget year')
                    new_condition = data[key_map[key]].is_in(values)
                else:
                    print(f"is {key_map[key]}")
                    print('values is not Budget_Year', values)
                    new_condition = data[key_map[key]].is_in(values)
                print(key_map[key], 'hiiiiii')
                group.append(key_map[key])

                if filter_condition  is None:
                    filter_condition = new_condition
                
                else:
                    filter_condition = filter_condition & new_condition
                
        except Exception as e:
            print(traceback.format_exc())
            print(f"Error applyting filters:{e}")
        if not sub_filter_state == True:
            print("Secondary filter apply :: subfilter state is false, group is empty")
            group = []
            filter_condition = None
            sub_filter_state = False
        else:
            print('Secondary filter is subfilter is true')
            print('group achieved is: ', group)


        return filter_condition,sub_filter_state,group

    
    def apply_heirarchial_filters(self,data: pl.DataFrame, group_by_id: Echelons,
                                sub_filter_state:bool, group:List,filter_condition:[Optional]=None) ->Union[None,pl.Series,bool,List]:
        
        print('we are applying hierarchial filter')
        group_map = {'family' : 'Family', 'sub_family' : 'SubFamily', 'supplier' : 'Supplier' , 'category' : 'Category', 'dom_comm' : 'DOM_COMM', 
        'sub_category' : 'SubCategory', 'extended_sub_category' : 'ExtendedSubCategory', 'sub_category_supplier' : 'SubCategorySupplier'}
        try:
            for key, values in group_by_id.dict().items(): 
                if not values: continue

                print(f'key is {key} & value is {values}')


                sub_filter_state = True
                new_condition = data[group_map[key]].is_in(values)
                
                group.append(group_map[key])

                if filter_condition is None:
                    filter_condition = new_condition
                else:
                    filter_condition = filter_condition & new_condition
        except Exception as e:
            print(traceback.format_exc())
            print(f"Error apply_heirarchial_filters:{e}")

        
        if not sub_filter_state == True:
            print("Secondary filter apply :: subfilter state is false, group is empty")
            group = []
            filter_condition = None
            sub_filter_state = False
        else:
            print('Secondary filter is subfilter is true')
            print('group achieved is: ', group)



        return filter_condition, sub_filter_state, group

 
    def calculate_quantity(self, data: pl.DataFrame):
        '''
        !!The operations are performed here after grouping!!
        Calculation on the Dataframe
            Args:
                df : DataFrame to be calculated
            Returns:
                data: Calculated data frame

        '''
       

        data = data.with_columns((pl.col("otb_amount")/pl.col("initial_average_retail_price").replace({np.inf:np.nan,-np.inf:np.nan}).replace(0,np.nan)).fill_nan(0).fill_null(0).alias("units_buy_by_sku"))
        data = data.with_columns(((pl.col("units_buy_by_sku") + pl.col("quantity_actuals")).fill_nan(0).fill_null(0).alias("quantity_act_forecast")))
        data = data.with_columns(pl.col("quantity_act_forecast").fill_null(0).alias("quantity_act_forecast"))
        data = data.with_columns(((pl.col("units_buy_by_sku")*100) / pl.col("quantity_act_forecast").replace({np.inf:0,-np.inf:0}).fill_nan(0).fill_null(0)).fill_nan(0).fill_null(0).alias("quantity_act_forecast_vs_otb_percent"))
        data = data.with_columns(((pl.col("quantity_act_forecast")*100)/pl.col("total_sku_count").replace(0,np.nan)).fill_null(0).alias("quantity_act_forecast _vs_sku"))
        data = data.with_columns(((pl.col("quantity_act_forecast")*100) / pl.col("quantity_ppy").replace(0,np.nan).replace([np.inf,-np.inf],np.nan)).fill_null(0).fill_nan(0).alias("quantity_act_forecast_vs_ppy_percent"))
        data = data.with_columns((pl.col("units_buy_by_sku")/pl.col("total_sku_count").replace(0,np.nan)).fill_null(0).alias("otb_vs_sku_qty"))        
        data = data.with_columns(((pl.col("units_buy_by_sku")*100) / pl.col("quantity_ppy").replace(0,np.nan)).fill_null(0).fill_nan(0).alias("otb_vs_qty_ppy_percent"))
        return data
    
    def calculate_cost(self, data: pl.DataFrame):
            '''
            !!The operations are performed here after grouping!!
            Calculation on the Dataframe
                Args:
                    df : DataFrame to be calculated
                Returns:
                    data: Calculated data frame

            '''

            data = data.with_columns(pl.col("otb_cost").fill_null(0).alias("otb_cost"))
            data = data.with_columns(((pl.col("otb_cost") * 100) / pl.col("otb_cost").sum()).fill_null(0).alias("otb_cost_percent"))
            data = data.with_columns((pl.col("otb_cost") + pl.col("cost_actuals")).fill_null(0).alias("cost_act_forecast"))
            data = data.with_columns(((pl.col("otb_cost")*100) / pl.col("cost_act_forecast").replace(0,np.nan)).fill_nan(0).fill_null(0).alias("otb_vs_act_forecast_cost_percent"))
            data = data.with_columns(((pl.col("cost_act_forecast")*100) / pl.col("cost_of_goods_ly").replace(0, np.nan)).fill_nan(0).fill_null(0).alias("cost_act_forecast_vs_ly_percent"))
            data = data.with_columns(((pl.col("cost_act_forecast")*100) / pl.col("total_sku_count").replace(0,np.nan)).fill_null(0).alias("cost_act_forecast _vs_sku"))
            data = data.with_columns(((pl.col("cost_act_forecast")*100) / pl.col("cost_of_goods_lly").replace(0,np.nan)).fill_nan(0).fill_null(0).alias("cost_act_forecast_vs_ppy_percent"))
            data = data.with_columns(((pl.col("otb_cost") / pl.col("total_sku_count").replace(0,np.nan))).fill_null(0).alias("cost_otb_vs_sku"))
            data = data.with_columns((((pl.col("otb_cost")*100)/pl.col("cost_of_goods_ly").replace(0,np.nan)).fill_nan(0)).fill_null(0).alias("otb_vs_py_cost_percent"))
            data = data.with_columns(((pl.col("otb_cost")*100)/pl.col("cost_of_goods_lly").replace(0,np.nan)).fill_nan(0).fill_null(0).alias("otb_vs_ppy_cost_percent"))
            data = data.with_columns(((pl.col("cost_of_goods_ly") * 100) / pl.col("cost_of_goods_ly").sum()).fill_nan(0).fill_null(0).alias("cost_mix_percent"))

            return data 
            






    def calculate_margin(self, data: pl.DataFrame):
            '''
            !!The operations are performed here after grouping!!
            Calculation on the Dataframe
                Args:
                    df : DataFrame to be calculated
                Returns:
                    data: Calculated data frame

            '''

            print(data["otb_amount"].sum(),"SUMM")
            data = data.with_columns(((pl.col("otb_amount") - pl.col("otb_cost")) / pl.col("otb_amount").replace(0,np.nan)).fill_nan(0).fill_null(0).alias("otb_gross_margin"))
            data = data.with_columns(((pl.col("sales_actual") - pl.col("cost_actuals"))/pl.col("sales_actual").replace(0,np.nan)).fill_nan(0).fill_null(0).alias("margin_actuals"))
            data = data.with_columns((pl.col("otb_gross_margin")/100).fill_nan(0).fill_null(0).alias("otb_gross_margin_percent"))
            data = data.with_columns(((pl.col("otb_gross_margin")*100)/(pl.col("otb_gross_margin").sum())).fill_null(0).alias("otb_margin_mix_percent"))
            data = data.with_columns(((pl.col("otb_gross_margin")*100)/pl.col("otb_amount")).fill_null(0).alias("otb_gross_margin_percent"))
            data = data.with_columns((pl.col("otb_gross_margin").fill_nan(0) +pl.col("margin_actuals")).fill_null(0).alias("margin_act_forecast"))
            data = data.with_columns(((pl.col("otb_gross_margin")* 100) / pl.col("margin_act_forecast").replace(0,np.nan)).fill_null(0).alias("Otb_vs_Act/Forecast_Margin%"))
            data = data.with_columns(((pl.col("net_sales_ly") - pl.col("cost_of_goods_ly"))/pl.col("net_sales_ly").replace(0,np.nan)).fill_nan(0).fill_null(0).alias("ly_margin_percent"))
            data = data.with_columns(((pl.col("margin_act_forecast")*100)/pl.col("ly_margin_percent").replace(0,np.nan)).fill_nan(0).fill_null(0).alias("margin_act_forecast_vs_ly_percent"))
            data = data.with_columns(((pl.col("margin_act_forecast")*100/pl.col("total_sku_count"))).fill_nan(0).fill_null(0).alias("margin_act_forecast_vs_sku"))
            data = data.with_columns(((pl.col("net_sales_lly") - pl.col("cost_of_goods_lly"))/pl.col("net_sales_lly").replace(0,np.nan)).fill_nan(0).fill_null(0).alias("lly_margin_percent"))
            data = data.with_columns(((pl.col("margin_act_forecast")*100)/pl.col("lly_margin_percent").replace(0,np.nan)).fill_nan(0).fill_null(0).alias("margin_act_forecast_vs_ppy_percent"))
            data = data.with_columns((pl.col("otb_gross_margin")/pl.col("total_sku_count").replace(0,np.nan)).fill_null(0).alias("margin_otb_vs_sku"))
            data = data.with_columns((pl.col("otb_gross_margin")/pl.col("ly_margin_percent").replace(0,np.nan)).fill_nan(0).fill_null(0).alias("otb_vs_py_margin_percent"))
            data = data.with_columns(((pl.col("otb_gross_margin")*100)/pl.col("lly_margin_percent").replace(0,np.nan)).fill_nan(0).fill_null(0).alias("otb_vs_ppy_margin_percent"))
            data = data.with_columns(((pl.col("margin_actuals")*100)/(pl.col("margin_actuals").sum())).fill_nan(0).fill_null(0).alias("margin_mix_percent"))
            return data
    
    def second_grouping(self,data,ds):


        """
    This function performs the second stage of hierarchical grouping and data filtering based on specific columns.

    Args:
        data: The main DataFrame containing the initial data.
        ds: The filtered DataFrame from the previous stage.

    Returns:
        Tuple[pd.DataFrame, bool]:
            - A new DataFrame containing the further filtered data based on hierarchical grouping.
            - A boolean flag indicating whether the "Channel" column is present in the final filtered DataFrame.

    Raises:
        Exception: If any unexpected error occurs.

    This function performs the following steps:

    1. Identifies columns for primary and secondary grouping based on predefined attributes.
    2. Extracts values from primary and secondary grouping columns based on the first row of the DataFrame.
    3. Determines the last filter applied based on the hierarchy of grouping columns.
    4. Checks if the first grouping column is "Channel" (if not, returns the current DataFrame).
    5. If there are any grouping columns:
        - Applies additional filters based on secondary grouping columns and their values.
        - If any filtered data remains, updates the `ds` DataFrame and sets the `Channel_flag` to True.
    6. If no filter applies or results in an empty DataFrame, returns the current `ds` DataFrame and the `Channel_flag` as False.

    This function is responsible for further refining the data based on hierarchical grouping logic defined within the class.

        """
        print(20)
        Channel_flag = False
        subs_count = 0
        group = []
        for col in data.columns:
            if col in self.max_col:
                group.append(col)
        try:
            sub_group = []
            sub_val = []
            cols = []
            vals = []

            if len(data)>0:
                temp = data.loc[0]
                for col in group:
                    if col not in self.heirarchy:
                        sub_group.append(col)
                        sub_val.append(temp[col])
                    else:
                        cols.append(col)
                        vals.append(temp[col])

                group = cols+sub_group[::-1]

                last_filter = 0
                if len(cols)>0:
                    last_filter = self.heirarchy.index(cols[-1])
                    if last_filter < len(self.heirarchy)-1:
                        last_filter = self.heirarchy.index(cols[-1]) + 1
                    else:
                        last_filter = self.heirarchy.index(cols[-1])
                elif len(sub_group)>0:
                    last_filter = 0
                if len(group)>0:
                    if group[0] != "Channel":
                        return ds,Channel_flag
                if len(group) == 0:
                    return ds,Channel_flag
                print(group)

                if len(group)>1:
                    filter = None
                    for col,val in zip(cols[1:]+sub_group,vals[1:]+sub_val):
                        if filter is None:
                            if col == "Otb date":
                                ds[col]=ds[col].astype(str)
                                filter = ds[col]  == str(val)
                            else:
                                filter = ds[col]  == val
                        else:
                            if col == "Otb date":
                                ds[col]=ds[col].astype(str)
                                filter = filter & (ds[col] == str(val))
                            else:
                                filter = filter & (ds[col] == val)
                            
                    if not ds[filter].empty:
                        ds = ds[filter]

                Channel_flag = True
                ds = self.calculate_initial_frame(ds,["ITEMID"])
                return ds,Channel_flag
            else:
                return ds,Channel_flag


        except:
            print(traceback.format_exc())




   
    def calculate_bottom_first_tab(self,data):
        data = data.with_columns(pl.col("initial_average_retail_price").alias("initial_average_retail_price"))
        data = data.with_columns(pl.col("otb_amount")*100/pl.col("initial_average_retail_price").alias("units_buy_by_sku"))
        data = data.with_columns(pl.col("otb_amount")/pl.col("total_sku_count").alias("otb_vs_sku"))
        data = data.with_columns(((pl.col("otb_amount")*100) / pl.col("sales_act_vs_forecast")).alias("otb_vs_act_forecast_percent"))
        data = data.with_columns(pl.col("net_sales_ly").alias("net_sales_ly"))
        data = data.with_columns(pl.col("otb_amount")*100/pl.col("net_sales_ly").alias("otb_vs_py_percent"))
        data = data.with_columns(pl.col("net_sales_lly").alias("net_sales_lly"))
        return data

        
    
    def calculate_bottom_qty_tab(self,data):
        
        data = data.with_columns((pl.col("units_buy_by_sku")*100/pl.col("quantity_act_forecast")).alias("quantity_act_forecast_vs_otb_percent"))
        data = data.with_columns((pl.col("quantity_act_forecast")*100/pl.col("total_sku_count")).replace(0, np.nan).alias("quantity_act_forecast _vs_sku"))
        data = data.with_columns(pl.col("quantity_ppy").alias("quantity_ppy"))
        data = data.with_columns((pl.col("units_buy_by_sku")/pl.col("total_sku_count")).fill_nan(0).alias("otb_vs_sku_qty"))
        data = data.with_columns(otb_vs_qty_ppy_percent = (pl.col("units_buy_by_sku")*100/pl.col("quantity_ppy")))
        return data


   
    def calculate_bottom_cost_tab(self,data):
        data = data.with_columns(pl.col("cost_actuals").alias("cost_actuals"))
        data = data.with_columns(cost_act_forecast = pl.col("otb_cost") + pl.col("cost_actuals"))
        data = data.with_columns(((pl.col("otb_cost")*100) / pl.col("cost_act_forecast")).alias("otb_vs_act_forecast_cost_percent"))
        data = data.with_columns(((pl.col("cost_act_forecast") / pl.col("total_sku_count"))*100).alias("cost_act_forecast _vs_sku"))
        data = data.with_columns(((pl.col("cost_act_forecast")*100) / pl.col("cost_of_goods_lly").replace(0, np.nan)).alias("cost_act_forecast_vs_ppy_percent"))
        data = data.with_columns(pl.col("otb_cost")/pl.col("total_sku_count"))
        data = data.with_columns(((pl.col("otb_cost")*100)/pl.col("cost_of_goods_ly")).alias("otb_vs_py_cost_percent"))
        data = data.with_columns(((pl.col("otb_cost")*100)/pl.col("cost_of_goods_lly")).alias("otb_vs_ppy_cost_percent"))

        return data

    def calculate_bottom_margin_tab(self,data):
    
        data = data.with_columns((((pl.col("sales_actual")) - pl.col("cost_actuals"))/pl.col("sales_actual")).alias("margin_actuals"))
        data = data.with_columns(((pl.col("otb_gross_margin")*100)/pl.col("otb_amount")).fill_nan(0).alias("otb_gross_margin_percent")) 
        data = data.with_columns((pl.col("otb_gross_margin")+pl.col("margin_actuals")).fill_nan(0).alias("margin_act_forecast"))
        data = data.with_columns(((pl.col("otb_gross_margin")*100) / pl.col("margin_act_forecast")).alias("otb_vs_act_forecast_margin_percent"))
        data = data.with_columns(((pl.col("margin_act_forecast")*100)/pl.col("ly_margin_percent")).fill_nan(0).alias("margin_act_forecast_vs_ly_percent"))
        data = data.with_columns((pl.col("margin_act_forecast")/pl.col("total_sku_count")*100).alias("margin_act_forecast_vs_sku"))
        data = data.with_columns(((pl.col("margin_act_forecast")*100)/pl.col("lly_margin_percent")).alias("margin_act_forecast_vs_ppy_percent"))
        data = data.with_columns((pl.col("otb_gross_margin")/pl.col("total_sku_count")).alias("margin_otb_vs_sku"))
        data = data.with_columns((pl.col("otb_gross_margin")/pl.col("ly_margin_percent")).fill_nan(0).alias("otb_vs_py_margin_percent"))
        data = data.with_columns(((pl.col("otb_gross_margin")*100)/pl.col("lly_margin_percent")).alias("otb_vs_ppy_margin_percent"))
   
        return data


    def save_table_to_db(self,df:pl.DataFrame,db:Session,table_name:str):
        max_icols = ['Description','Department','Category','Family','SubFamily','SubCategory','ExtendedSubCategory',
                    'SubCategorySupplier','AssemblyCodeNickName','ENDOFLife','DOM_COMM','Status',"Store","area","Region",
                    "Budget_Year",
                'budget_quarter','budget_month','budget_week','budget_day',"Supplier"]
        # print(df.to_pandas().columns.to_list(),"LIST")
        avg_icols =["otb_gross_margin_percent"]
        sumi_cols = ['otb_amount','otb_cost',"units_buy_by_sku"]
        subset =["ITEMID","Channel","INVENTLOCATIONID","Budget_date"]
        bp_columns_1 ={'Description':'Description','Department':'Department','Category':'Category','Family':'Family',
        'SubFamily':'SubFamily','SubCategory':'SubCategory','ExtendedSubCategory':'ExtendedSubCategory',
                    'SubCategorySupplier':'SubCategorySupplier','AssemblyCodeNickName':'AssemblyCodeNickName','ENDOFLife':'ENDOFLife',
                    'DOM_COMM':'DOM_COMM','Status':'Status','Store':'Store','area':'area','Region':'Region','Budget_Year':'budget_year',
                  'Supplier':'Supplier','ITEMID':'ITEMID','Channel':'Channel','INVENTLOCATIONID':'INVENTLOCATIONID','Budget_date':'budget_date',
        'otb_amount':'budget_amount','otb_cost':'budget_cost','units_buy_by_sku':"units_buy_by_sku",'otb_gross_margin_percent':'budget_gross_margin_percent'}
        
        # If user selected revised otb amount
        bp_columns_kpi ={'Description':'Description','Department':'Department','Category':'Category','Family':'Family',
        'SubFamily':'SubFamily','SubCategory':'SubCategory','ExtendedSubCategory':'ExtendedSubCategory',
                    'SubCategorySupplier':'SubCategorySupplier','AssemblyCodeNickName':'AssemblyCodeNickName','ENDOFLife':'ENDOFLife',
                    'DOM_COMM':'DOM_COMM','Status':'Status','Store':'Store','area':'area','Region':'Region','Budget_Year':'budget_year',
                  'Supplier':'Supplier','ITEMID':'ITEMID','Channel':'Channel','INVENTLOCATIONID':'INVENTLOCATIONID','Budget_date':'budget_date',
        'revised_otb_amount':'budget_amount','otb_cost':'budget_cost','units_buy_by_sku':"units_buy_by_sku",'otb_gross_margin_percent':'budget_gross_margin_percent'}

        agg_dict =[eval(expr) for expr in [f"pl.col('{col}').mean()" for col in avg_icols]
                    +[f"pl.col('{col}').sum()" for col in sumi_cols] +
                    [f"pl.col('{col}').max()" for col in max_icols]]

        df = df.groupby(subset).agg(agg_dict)
        df = df.to_pandas()

        columns_1 =['Description','Department','Category','Family','SubFamily','SubCategory','ExtendedSubCategory',
                    'SubCategorySupplier','AssemblyCodeNickName','ENDOFLife','DOM_COMM','Status',
                    'Store','area','Region', 'Budget_Year',
                    'Supplier','ITEMID','Channel','INVENTLOCATIONID','Budget_date',
        'otb_amount','otb_cost',"units_buy_by_sku",'otb_gross_margin_percent']
        # If user selected revised otb
        columns_kpi =['Description','Department','Category','Family','SubFamily','SubCategory','ExtendedSubCategory',
                    'SubCategorySupplier','AssemblyCodeNickName','ENDOFLife','DOM_COMM','Status',
                    'Store','area','Region', 'Budget_Year',
                    'Supplier','ITEMID','Channel','INVENTLOCATIONID','Budget_date',
        'revised_otb_amount','otb_cost',"units_buy_by_sku",'otb_gross_margin_percent']

        if 'revised_otb_amount' in df.columns:
            df = df[columns_kpi]
            df = df.rename(columns =bp_columns_kpi)
            
        else:
            df = df[columns_1]
            df = df.rename(columns =bp_columns_1)
        
        # df = df.rename(columns =bp_columns)
        # df.to_csv(f"{table_name}.tsv",header=True,index=False,sep='\t')
        # df.to_sql(table_name,con=db.bind,if_exists='replace',index=False,method=None, chunksize=5000)
        # db.commit()
        #-------------------------------------------
        # df = df.to_csv(f"{table_name}.tsv",header=True,index=False,sep='\t')
        # try:
        #     with db.bind.raw_connection().cursor() as cursor:
        #         try:
        #             cursor.execute(f"TRUNCATE TABLE {table_name};")
        #             with open(f'{table_name}.tsv','r',) as file:
        #                 cursor.execute("BEGIN;")
        #                 cursor.copy_expert(sql = f"COPY {table_name} FROM STDIN WITH DELIMITER E'\t' CSV HEADER;",file=file)
        #                 cursor.execute("COMMIT;")
        #         except:
        #             print(traceback.format_exc())
        #             cursor.connection.rollback()
        #         finally:
        #             cursor.close()
        #             os.remove(f'{table_name}.tsv')
        # except:
        #     print(traceback.format_exc())
        #---------------------------------------------
        df.to_sql(table_name, con = db.bind, if_exists='replace', index = False, method=None, chunksize = 5000)
        db.commit()

    def call_kpi(self,df: pl.DataFrame,data_filter : Dict):

        """
        
        Calculate Coefficient scores for the given DataFrame.

        Parameters:
        - df (pandas.DataFrame): The DataFrame containing relevant data.
        - data_filter (dict): A dictionary containing filters for KPI calculations.
        - sku (int): The SKU value.
        - subset: The subset value.

        returns:
        pandas.DataFrame: A DataFrame with calculated KPI scores.
        """
        
        print('we are in kpi call')
        total = len(df)
        average_len = 0       
        df = df.with_columns((pl.lit(0.0)).alias("coefficient_score"))

        secondary_filter = data_filter['secondary_filter']
        if secondary_filter['article_score'] != []:
            try:
                
                for scores in secondary_filter['article_score']:
    #  ARTICLE_SCORES : list = ['sale', 'abc', 'ae', 'speed', 'terminal', 'margin', 'sell', 'markdown', 'core', 'quartile', 'sortimeter']
   
                    if scores == 'article_score_sale':
                        # print(df["article_score_sale"].mean(),"ARTICLESCORESALE")
                        average_len += 1
                        df = df.with_columns(((pl.col("coefficient_score") + pl.col("article_score_sale")).cast(pl.Float64).replace({np.inf:0,-np.inf:0}).fill_nan(0)).alias("coefficient_score"))
                        print(df["coefficient_score"].mean(),"ARTICLESCORESALE")

                    if scores == 'article_score_abc':
                        average_len += 1
                        df = df.with_columns(((pl.col("coefficient_score") + pl.col("article_score_abc")).cast(pl.Float64).replace({np.inf:0,-np.inf:0}).fill_nan(0).fill_null(0)).alias("coefficient_score"))

                    if scores == 'article_score_ae':
                        average_len += 1
                        df = df.with_columns(((pl.col("coefficient_score") + pl.col("article_score_ae")).cast(pl.Float64).replace({np.inf:0,-np.inf:0}).fill_nan(0).fill_null(0)).alias("coefficient_score"))

                    if scores == 'article_score_speed':
                        average_len += 1
                        df = df.with_columns(((pl.col("coefficient_score") + pl.col("article_score_speed")).cast(pl.Float64).replace({np.inf:0,-np.inf:0}).fill_nan(0).fill_null(0)).alias("coefficient_score"))

                    if scores == 'article_score_terminal':
                        average_len += 1
                        df = df.with_columns(((pl.col("coefficient_score") + pl.col("article_score_terminal")).cast(pl.Float64).replace({np.inf:0,-np.inf:0}).fill_nan(0).fill_null(0)).alias("coefficient_score"))

                    if scores == 'article_score_margin':
                        average_len += 1
                        df = df.with_columns(((pl.col("coefficient_score") + pl.col("article_score_margin")).cast(pl.Float64).replace({np.inf:0,-np.inf:0}).fill_nan(0).fill_null(0)).alias("coefficient_score"))

                    if scores == 'article_score_sell':
                        average_len += 1
                        df = df.with_columns(((pl.col("coefficient_score") + pl.col("article_score_sell")).cast(pl.Float64).replace({np.inf:0,-np.inf:0}).fill_nan(0).fill_null(0)).alias("coefficient_score"))

                    if scores == 'article_score_markdown':
                        average_len += 1
                        df = df.with_columns(((pl.col("coefficient_score") + pl.col("article_score_markdown")).cast(pl.Float64).replace({np.inf:0,-np.inf:0}).fill_nan(0).fill_null(0)).alias("coefficient_score"))

                    if scores == 'article_score_core':
                        average_len += 1
                        df = df.with_columns(((pl.col("coefficient_score") + pl.col("article_score_core")).cast(pl.Float64).replace({np.inf:0,-np.inf:0}).fill_nan(0).fill_null(0)).alias("coefficient_score"))

                    if scores == 'article_score_quartile':
                        average_len += 1
                        df = df.with_columns(((pl.col("coefficient_score") + pl.col("article_score_quartile")).cast(pl.Float64).replace({np.inf:0,-np.inf:0}).fill_nan(0).fill_null(0)).alias("coefficient_score"))

                    if scores == 'article_score_sortimeter':
                        average_len += 1  
                        df = df.with_columns(((pl.col("coefficient_score") + pl.col("article_score_sortimeter")).cast(pl.Float64).replace({np.inf:0,-np.inf:0}).fill_nan(0).fill_null(0)).alias("coefficient_score"))
              
                df = df.with_columns(((pl.col("coefficient_score")/average_len).cast(pl.Float64).replace({np.inf:0,-np.inf:0}).fill_nan(0).fill_null(0)).alias("coefficient_score"))
                df = df.with_columns(coefficient_score_mix_percent = (pl.col('coefficient_score')/pl.col('coefficient_score').sum()).replace({np.inf:0,-np.inf:0}).fill_nan(0).fill_null(0) * 100)

            except Exception as e:

                print(f"Error:{e}")
                print(traceback.format_exc())

        return df

    def table_change_filter(self,group:List,heirarchy:List,data_filter:Dict,DATA:pl.DataFrame,row:Dict,
                        filter_condition:Optional[pl.Series]=None) -> Union[None,pl.Series,List,pl.DataFrame]:

        columns_to_filter = []
        values_to_filter = []

        for i in group+heirarchy:
            if i in row:
                columns_to_filter.append(i)
                values_to_filter.append(row[i])
            
        child = filter_condition
        other_filter_condition = None
        parent = None
        for col, val in zip(columns_to_filter, values_to_filter):
            print('col is', col, 'and val is', val)
            if child is None:
                print('child is none')
                child = (DATA[col] == val)
                other_filter_condition = ~(DATA[col] == val)
                parent = None

            else:
                parent = child
                other_filter_condition = child & (DATA[col] != val)
                child = child & (DATA[col] == val)
        return child,other_filter_condition,filter_condition,parent,columns_to_filter,values_to_filter,group,DATA


# sum totals
# sales_actual, net_sales_ly, net_sales_lly, budget_amount, budget_act_fct, budget_qty, coefficient_score_max_percent
# Line total
# otb_vs_act_Forecast_percent, otb_vs_py_percent, otb_vs_ppy_percent, initial_average_retail_price, units_buy_by_sku, total_sku_count, otb_vs_sku, act_forecast_vs_ly_percent, coefficient_score
