from pydantic import BaseModel
from typing import List,Dict
from sqlalchemy.orm import Session
from sqlalchemy import text
import pandas as pd
import numpy as np
import os
import datetime
import calendar
from decimal import Decimal
from celery.result import AsyncResult
from time import ctime


import psycopg2
import math
import sys
import polars as pl

from typing import List,Dict,Optional,Union,Tuple
from fastapi import Depends

from rapidframes import RapiDF
from .schemas import  Filters
import traceback





class Parameters:

  # article count taken from budger items . since there is no ly budget details no ly count
  # sales ly getting is for current budget items count

    
    other_cols = [ "ly_margin_percent","lly_margin_percent",'cum_sum',"sold_article_count_ly","top_a_class","tail","top_and_normal",
                "top_80_percent_to_total_articles","tail_percent_to_total_articles","top_and_normal_percent_to_total_article","total_sku_article_buy",
                "opening_sku_article_count","closing_sku_article_count","sku_article_mix_percent","sku_article_turn","sku_article_efficency",
                "current_stock_on_hand_retail_value","initial_gp_percent","final_gp_percent","markdown_percent","stock_turn","sellthrough_percent",
                "st_greater_than_70","st_greater_than_70_percent_mix","out_of_stock_percent",
                "plan_for_otb_value","plan_for_net_sales_value","plan_for_sku_article_count","plan_for_net_qty",
                "plan_for_purchase_qty","plan_for_net_sales_mix_percent","normalised_sales_mix_final_percent","normalised_sku_article_mix_final_percent",
                "fcst_net_sales_value","fcst_net_sales_value_sales_mix_percent","fcst_sku_article","fcst_sku_article_mix_percent",
                "fcst_otb_value","fcst_otb_quantity","fcst_otb_value_at_cost","temp_purchase","temp_sold","stock_on_hand_qty_mix_percent",
                "current_stock_on_hand_cost_value","current_stock_cost",
                "budget_vpy","budget_vppy","total_sku_count","units_per_sku","budget_per_sku",
                'stock_adjustment_qty(+/-)',"act_forecast_vs_budget_percent","sales_act_or_forecast_sku_article","act_forecast_vs_ly_percent",
                "act_forecast_vs_ppy_percent","speed_of_sales"]

    columns = ["Check_box","Channel",'country',"Region",'area','city',"StoreId",
                "Store",'season',"Department","Family","SubFamily","Category","DOM COMM","SubCategory",
                "ExtendedSubCategory","Supplier","SubCategorySupplier","AssemblyCodeNickName","Status","ENDOFLife",
                "Description","ITEMID","Budget_Year",
                'budget_quarter','budget_month','budget_week','budget_day','Budget_date',"historical_year",'history_quarter',
                'history_month','history_week',"history_day","History_date",
                "budget_amount",'revised_budget_amount','new_budget_mix', 
                "budget_percent","relative_budget_percent","budget_vpy","budget_vppy",
                "total_sku_count","units_per_sku","budget_per_sku", 
                "budget_gross_margin_percent","adjusted_budget_gross_margin_percent",
                "budget_cost","budget_qty","sales_actual","sales_act_forecast","act_forecast_vs_budget_percent","sales_act_or_forecast_sku_article",
                "act_forecast_vs_ly_percent","ly_margin_percent","net_sales_ly","gross_sales_ly","cost_of_goods_ly","sold_qty_ly","sold_article_count_ly",
                "net_sales_lly","cost_of_goods_lly","lly_margin_percent","act_forecast_vs_ppy_percent","initial_average_retail_price","final_price",
                "opening_sku_article_count","closing_sku_article_count","sku_article_mix_percent","sku_article_turn",
                "total_sku_article_buy","sku_article_efficency",
                "opening_stock","closing_stock","stock_received_qty",'stock_adjustment_qty(+/-)',
                "total_purchase_qty","purchase_value","purchase_value_mix_percent","stock_on_hand_qty","stock_on_hand_qty_mix_percent","current_stock_cost_at_retail","current_stock_on_hand_retail_value","current_stock_cost",
                "current_stock_on_hand_cost_value",
                "net_sales_mix_percent","initial_gp_percent","final_gp_percent","sellthrough_percent","markdown_percent",
                "stock_turn","st_greater_than_70","st_greater_than_70_percent_mix","out_of_stock_percent","top_a_class","top_80_percent_to_total_articles","tail",
                "tail_percent_to_total_articles","top_and_normal","top_and_normal_percent_to_total_article","speed_of_sales","cum_sum",
                "plan_for_otb_value","plan_for_net_sales_value","plan_for_sku_article_count","plan_for_net_qty",
                "plan_for_purchase_qty","plan_for_net_sales_mix_percent","normalised_sales_mix_final_percent","normalised_sku_article_mix_final_percent",
                "fcst_net_sales_value","fcst_net_sales_value_sales_mix_percent","fcst_sku_article","fcst_sku_article_mix_percent",
                "fcst_otb_value","fcst_otb_quantity","fcst_otb_value_at_cost",
                'article_score_sale','article_score_abc','article_score_ae','article_score_speed','article_score_terminal',
                'article_score_margin','article_score_sell','article_score_markdown','article_score_core',
                'article_score_quartile','article_score_sortimeter','btech_vs_sortimeter','store_sku_count',
                "temp_purchase","temp_sold","coefficient_score","coefficient_score_mix_percent"]



    max_col = ["Channel","Family","SubFamily","Category","SubCategory","ExtendedSubCategory","Supplier",
                 "SubCategorySupplier","AssemblyCodeNickName",
                "Status","Description","ENDOFLife","ITEMID","Store",
                'history_month','history_week',"History_date",'history_quarter',"history_day","budget_quarter",
                'budget_month','budget_week','budget_day','Budget_date',"Check_box"
                ]
    rank_col = ["Check_box","order_index"     ]
    percent_cols = ["budget_percent","relative_budget_percent", "ly_margin_percent","lly_margin_percent",
                    "tail_percent_to_total_articles","top_and_normal_percent_to_total_article",
                    "sku_article_mix_percent","initial_gp_percent","final_gp_percent","markdown_percent",
                    "sellthrough_percent","st_greater_than_70_percent_mix","out_of_stock_percent",
                    "plan_for_net_sales_mix_percent","normalised_sales_mix_final_percent",
                    "normalised_sku_article_mix_final_percent","fcst_net_sales_value_sales_mix_percent",
                    "fcst_sku_article_mix_percent","stock_on_hand_qty_mix_percent","act_forecast_vs_budget_percent",
                    "act_forecast_vs_ly_percent","act_forecast_vs_ppy_percent","budget_cost_percent",
                    "budget_vs_act_forecast_cost_percent","budget_vs_py_cost_percent","budget_vs_ppy_cost_percent",
                    "cost_per_sku","cost_mix_percent","cost_act_forecast_vs_budget_percent","cost_act_forecast_vs_ly_percent",
                    "cost_act_forecast_vs_ppy_percent","budget_cost_percent","budget_vs_act_forecast_cost_percent",
                    "budget_vs_py_cost_percent","budget_vs_ppy_cost_percent",
                    "cost_per_sku","cost_mix_percent","cost_act_forecast_vs_budget_percent","cost_act_forecast_vs_ly_percent",
                    "cost_act_forecast_vs_ppy_percent","budget_margin_percent","budget_margin_mix_percent","margin_mix_percent",
                    "budget_vs_act_forecast_margin_percent","budget_vs_py_margin_percent",
                    "budget_vs_ppy_margin_percent","margin_act_forecast_vs_ly_percent","margin_act_forecast_vs_ppy_percent"
                ]

    sum_col =  ["budget_percent","relative_budget_percent","sales_act_forecast",
                "total_sku_count","units_per_sku","budget_per_sku",
                "budget_cost","budget_qty","sales_actual","budget_amount","net_sales_ly",
                "act_forecast_vs_ly_percent","sales_act_or_forecast_sku_article",
                "cost_of_goods_ly","net_sales_lly","cost_of_goods_lly",
                "opening_sku_article_count","closing_sku_article_count","sold_article_count_ly",
                "total_sku_article_buy",
                "opening_stock","closing_stock","stock_received_qty",'stock_adjustment_qty(+/-)',
                "total_purchase_qty","purchase_value","stock_on_hand_qty","current_stock_on_hand_retail_value",
                "current_stock_on_hand_cost_value",
                "sold_qty_ly","gross_sales_ly","st_greater_than_70","top_a_class","tail",
                "top_and_normal","plan_for_otb_value","plan_for_net_sales_value","plan_for_sku_article_count",
                "plan_for_net_qty","plan_for_purchase_qty","fcst_net_sales_value","fcst_sku_article","plan_for_net_sales_mix_percent",
                "fcst_otb_value","fcst_net_sales_value_sales_mix_percent","fcst_sku_article_mix_percent","purchase_value_mix_percent","net_sales_mix_percent",
                "sku_article_mix_percent"
                ,"normalised_sku_article_mix_final_percent","fcst_otb_quantity","normalised_sales_mix_final_percent","fcst_otb_value_at_cost",
                "temp_sold","temp_purchase",
                "quantity_actuals","quantity_ppy","budget_qty_percent","quantity_mix_percent","quantity_act_forecast","quantity_act_forecast_vs_budget_percent",
                "quantity_act_or_forecast_per_sku",
                "cost_actuals","budget_cost_percent","cost_per_sku","cost_mix_percent","cost_act_or_forecast_per_sku","cost_act_forecast",
                "cost_budget_per_sku",
                "margin_actuals","margin_act_or_forecast_per_sku","margin_mix_percent","margin_act_forecast"
                ,"budget_margin_percent","st_greater_than_70_percent_mix","budget_margin_mix_percent","stock_on_hand_qty_mix_percent","current_stock_cost_at_retail",
                "current_stock_cost",'btech_vs_sortimeter','store_sku_count',"coefficient_score_mix_percent","coefficient_score"
                ]


    avg_col = ["budget_vpy","budget_vppy","ly_margin_percent",
                "budget_gross_margin_percent","adjusted_budget_gross_margin_percent","lly_margin_percent","act_forecast_vs_ppy_percent","act_forecast_vs_budget_percent",
                "final_price",
                "initial_gp_percent","final_gp_percent","markdown_percent","sellthrough_percent","sku_article_efficency","sku_article_turn",
                "top_80_percent_to_total_articles","top_and_normal_percent_to_total_article","tail_percent_to_total_articles","stock_turn","speed_of_sales",
                "quantity_act_forecast_vs_ppy_percent","budget_vs_act_forecast_qty_percent","budget_vs_py_qty_percent","budget_vs_ppy_qty_percent",
                "budget_per_sku_qty",
                "budget_vs_act_forecast_cost_percent","budget_vs_py_cost_percent","budget_vs_ppy_cost_percent","cost_act_forecast_vs_budget_percent","cost_act_forecast_vs_ly_percent",
                "cost_act_forecast_vs_ppy_percent",
                "quantity_act_forecast_vs_ly_percent",
                "budget_gross_margin","margin_act_forecast_vs_ppy_percent","margin_act_forecast_vs_ly_percent"
                ,"margin_budget_per_sku","budget_vs_py_margin_percent",'initial_average_retail_price',"out_of_stock_percent",
                'article_score_sale','article_score_abc','article_score_ae','article_score_speed','article_score_terminal',
                'article_score_margin','article_score_sell','article_score_markdown','article_score_core',
                'article_score_quartile','article_score_sortimeter']

    
    int_cols = ["budget_qty","plan_for_otb_value","plan_for_net_sales_value","plan_for_sku_article_count","units_per_sku",
                "plan_for_net_qty","plan_for_purchase_qty","fcst_net_sales_value","fcst_sku_article","fcst_otb_value","fcst_otb_value_at_cost",
                "fcst_otb_quantity"]

    float_cols = ['initial_average_retail_price',"budget_gross_margin_percent","adjusted_budget_gross_margin_percent","budget_cost"]

    heirarchy = ["Channel","Family","SubFamily","Category","SubCategory","ExtendedSubCategory","Supplier",
                 "SubCategorySupplier","AssemblyCodeNickName","Status","Description","ENDOFLife","ITEMID"]
    
    editable_cols = ["budget_amount","budget_percent","units_per_sku","budget_per_sku","adjusted_budget_gross_margin_percent","budget_vpy","budget_vppy","budget_qty"
                    ,"act_forecast_vs_budget_percent","normalised_sku_article_mix_final_percent","normalised_sales_mix_final_percent"]



    quantity_cols = ["budget_qty_percent","budget_vs_act_forecast_qty_percent","budget_vs_py_qty_percent","budget_vs_ppy_qty_percent",
                    "budget_per_sku_qty","quantity_mix_percent",
                    "quantity_act_forecast","quantity_act_forecast_vs_budget_percent","quantity_act_forecast_vs_ly_percent",
                    "quantity_act_or_forecast_per_sku","quantity_act_forecast_vs_ppy_percent"]
    
    ra_quantity = ["Channel",'country',"Region",'area','city',"StoreId",
                    "Store",'season',"Department","Family","SubFamily","Category","DOM COMM","SubCategory",
                    "ExtendedSubCategory","Supplier","SubCategorySupplier","AssemblyCodeNickName","Status","ENDOFLife",
                    "Description","ITEMID",
                    "budget_qty","budget_qty_percent","budget_vs_act_forecast_qty_percent","budget_vs_py_qty_percent","budget_vs_ppy_qty_percent",
                    "initial_average_retail_price","total_sku_count","units_per_sku","budget_per_sku_qty","quantity_mix_percent","quantity_actuals",
                    "quantity_act_forecast","budget_amount","quantity_act_forecast_vs_budget_percent","quantity_act_forecast_vs_ly_percent",
                    "quantity_act_or_forecast_per_sku","quantity_ppy","quantity_act_forecast_vs_ppy_percent"]
                    

    cost_cols = ["budget_cost_percent","budget_vs_act_forecast_cost_percent","budget_vs_py_cost_percent","budget_vs_ppy_cost_percent",
                    "cost_per_sku","cost_mix_percent","cost_act_forecast_vs_budget_percent","cost_act_forecast_vs_ly_percent",
                    "cost_act_forecast_vs_ppy_percent",
                    "cost_act_or_forecast_per_sku","cost_budget_per_sku","cost_act_forecast"]

                  


    ra_cost = ["Channel",'country',"Region",'area','city',"StoreId",
                "Store",'season',"Department","Family","SubFamily","Category","DOM COMM","SubCategory",
                "ExtendedSubCategory","Supplier","SubCategorySupplier","AssemblyCodeNickName","Status","ENDOFLife",
                "Description","ITEMID",
                "budget_cost","budget_cost_percent","budget_vs_act_forecast_cost_percent","budget_vs_py_cost_percent","budget_vs_ppy_cost_percent",
                "initial_average_retail_price","total_sku_count","cost_per_sku","cost_budget_per_sku","cost_mix_percent","cost_actuals",
                "cost_act_forecast","budget_amount","cost_act_forecast_vs_budget_percent","cost_act_forecast_vs_ly_percent",
                "cost_act_or_forecast_per_sku","cost_of_goods_lly","cost_act_forecast_vs_ppy_percent"]
                


    margin_cols = ["budget_gross_margin","margin_actuals","budget_margin_percent","budget_margin_mix_percent",
                    "budget_vs_act_forecast_margin_percent","budget_vs_py_margin_percent","budget_vs_ppy_margin_percent",
                    "buy_margin_per_sku","margin_budget_per_sku","margin_mix_percent",
                    "margin_act_forecast","margin_act_forecast_vs_ly_percent",
                    "margin_act_or_forecast_per_sku","margin_act_forecast_vs_ppy_percent"]

      

                

    ra_margin = ["Channel",'country',"Region",'area','city',"StoreId",
                "Store",'season',"Department","Family","SubFamily","Category","DOM COMM","SubCategory",
                "ExtendedSubCategory","Supplier","SubCategorySupplier","AssemblyCodeNickName","Status","ENDOFLife",
                "Description","ITEMID",
                "budget_gross_margin","budget_margin_percent","budget_margin_mix_percent","budget_vs_act_forecast_margin_percent","budget_vs_py_margin_percent","budget_vs_ppy_margin_percent",
                "initial_average_retail_price","buy_margin_per_sku","total_sku_count","margin_budget_per_sku","margin_mix_percent","margin_actuals","ly_margin_percent","lly_margin_percent",
                "margin_act_forecast","budget_amount","margin_act_forecast_vs_ly_percent",
                "margin_act_or_forecast_per_sku","margin_act_forecast_vs_ppy_percent"]
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
                    'historical_year':[],'history_Quarter':[],'history_month':[],'history_week':[],'history_Day':[],'history_dates':[],'article_score':[]}
    filter_store = dict(zip(['Store','Region','Channel','Budget_Year','budget_quarter','budget_month','budget_week','budget_day','Budget_date','historical_year','history_quarter'
                        ,'history_month','history_week','history_day',"History_date"],[[]]*15))

    non_article_cols = ["budget_vpy","budget_vppy","units_per_sku","budget_per_sku","budget_gross_margin_percent","adjusted_budget_gross_margin_percent",
                        "budget_cost","budget_qty","out_of_stock_percent","fcst_otb_quantity","initial_gp_percent","final_gp_percent","act_forecast_vs_ly_percent",
                        "speed_of_sales","sellthrough_percent","sku_article_turn","sku_article_efficency","current_stock_cost_at_retail","current_stock_on_hand_retail_value","current_stock_cost",
                       "current_stock_on_hand_cost_value"]
    group_subset = {"ITEMID","Channel",'Store','Region','Channel','Budget_Year','budget_quarter','budget_month','budget_week','budget_day','Budget_date','historical_year','history_quarter'
                    ,'history_month','history_week','history_day',"History_date"}

    time_columns = ['history_month','history_week',"History_date",'history_quarter',"history_day","budget_quarter",
                    'budget_month','budget_week','budget_day','Budget_date',"ITEMID","Channel"]
    kpi_metrics = ['article_score_sale','article_score_abc','article_score_ae','article_score_speed','article_score_terminal',
                    'article_score_margin','article_score_sell','article_score_markdown','article_score_core',
                    'article_score_quartile','article_score_sortimeter']
    kpi_selections = []

    SCORES: dict      = {'sale':'article_score_sale', 'abc' : 'article_score_abc', 'ae':'article_score_ae', 'speed': 'article_score_speed', 'terminal' : 'article_score_terminal', 'margin' : 'article_score_margin', 
                        'sell' : 'article_score_sell', 'markdown':'article_score_markdown', 'core' :'article_score_core', 'quartile':'article_score_quartile', 'sortimeter':'article_score_sortimeter'}
 

gloabal_vars = Parameters()

class Budget():
    columns = gloabal_vars.columns
    other_cols = gloabal_vars.other_cols
    max_col = gloabal_vars.max_col
    sum_col = gloabal_vars.sum_col 
    avg_col = gloabal_vars.avg_col
    int_cols = gloabal_vars.int_cols
    float_cols = gloabal_vars.float_cols
    heirarchy = gloabal_vars.heirarchy
    tabs = gloabal_vars.tabs
    SUB_FILTER = gloabal_vars.SUB_FILTER
    editable_cols = gloabal_vars.editable_cols
    non_article_cols = gloabal_vars.non_article_cols
    filter_store = gloabal_vars.filter_store
    group_subset = gloabal_vars.group_subset


    DATA              = pl.DataFrame()




    def initial_frame_calculation(self,df:pl.DataFrame) ->Tuple[pl.DataFrame,pl.DataFrame]:

        """
        Perform initial frame calculation.

        Parameters:
            df (pl.DataFrame): Input polars DataFrame.

        Returns:
            Tuple[pl.DataFrame, pl.DataFrame]: Processed polars DataFrames.
        """

        subset = ['ITEMID','Channel',"INVENTLOCATIONID","Budget_date","History_date"]
        df = self.calculate_initial_frame(df,subset)
        df = df.with_columns(Check_box = pl.lit(0))
        df = df.with_columns(coefficient_score_mix_percent = pl.lit(0.0))
        df = df.with_columns(coefficient_score = pl.lit(0.0))
        df = df.with_columns(order_index=pl.struct(subset).rank(method='ordinal'))
        # df = df.with_columns(revised_budget_amount= pl.lit(0.0))
        ds = df[self.max_col+["sold_qty_ly"]] 
        group = []
        self.SUB_FILTER = self.call_filter(df,self.SUB_FILTER,group,df,self.filter_store,{})
        self.DATA = self.calculate_df(df)
        return self.DATA,ds




    def call_filter(self,data:pl.DataFrame,SUB_FILTER:dict,group:List,DATA:pl.DataFrame,filter_store:Dict,data_filter:Dict) ->Dict:

        '''
        Updates the filter options based on the selected group and stores filter values in filter_store.

        Args:
            data (pl.DataFrame): Filtered dataframe.
            SUB_FILTER (dict): Dictionary to store filter options.
            group (List): List of selected group columns/sub filter columns.
            DATA (pl.DataFrame): DataFrame containing the original data.
            filter_store (Dict): Dictionary to store filter values.

        Returns:
            Dict: Updated SUB_FILTER dictionary.

        '''


        try:
            # Define mapping between column names and response names
            keys = ['store','region','Channel','budget_year','Quarter','month','week','Day','date','history_year','history_Quarter',
            'history_month','history_week','history_Day','history_dates']
            values = ['Store','Region','Channel','Budget_Year','budget_quarter','budget_month','budget_week','budget_day','Budget_date','historical_year','history_quarter'
            ,'history_month','history_week','history_day',"History_date"]

            map_list = []
            filter_dict = dict(zip(values,keys))
            # Initialize last_slection to track the last selected group column
            last_slection = ''
            # Check if group is not empty
            if group !=[]:
                last_slection = group[-1]
            print(last_slection,"last_selection")
            for i,v in zip(keys,values):
                # Update SUB_FILTER with unique values of the column and Store the filter values in filter_store
                if (v not in group) or (v in self.heirarchy):
                    if v in ['Budget_Year']:
                        SUB_FILTER[i]        = sorted(list(data[v].cast(pl.Float32).unique()),key=lambda x:(x is None,x))
                    elif v in ['Budget_date',"History_date"]:
                        SUB_FILTER[i]   = sorted(list(data[v].unique()),key=lambda x:(x is None,x))
                    elif v in ['budget_month','history_month']:
                        SUB_FILTER[i]   = sorted(list(data[v].cast(pl.Utf8).unique()),key=lambda x:(x is None,list(calendar.month_name).index(x.strip()) if x !=np.nan else None))
                    else:
                        SUB_FILTER[i]        = sorted(list(data[v].unique()),key=lambda x:(x is None,x))
                    SUB_FILTER[i] = self.remove_special_values(SUB_FILTER[i])
                    filter_store[v] = SUB_FILTER[i]
                else:
                    #if column is not last selected and group is empty then fethc subfilters form full data else from  filter store
                    if v in ['Budget_Year'] and v!= last_slection:
                        if group == []:
                            SUB_FILTER[i]        = sorted(list(DATA[v].cast(pl.Float32).unique()),key=lambda x:(x is None,x))
                        else:
                            SUB_FILTER[i] = filter_store[v]

                    elif v in ['Budget_date',"History_date"] and v != last_slection:
                        if group == []:
                            SUB_FILTER[i]   = sorted(list(DATA[v].cast(pl.Utf8).unique()),key=lambda x:(x is None,x))
                        else:
                            SUB_FILTER[i] = filter_store[v]

                    elif v in ['budget_month','history_month'] and v != last_slection:
                        if group == []:
                            SUB_FILTER[i]   = sorted(list(DATA[v].cast(pl.Utf8).unique()),key=lambda x:(x is None,list(calendar.month_name).index(x.strip()) if not pd.isna(x) else 0))
                        else:
                            SUB_FILTER[i] = filter_store[v]
                    else:
                        if v!=last_slection:
                            if group == []:
                                SUB_FILTER[i]        = sorted(list(DATA[v].unique()),key=lambda x:(x is None,x))
                            else:
                                SUB_FILTER[i] = filter_store[v]
                    SUB_FILTER[i] = self.remove_special_values(SUB_FILTER[i])



            # for mapping historical and budget days,months,quarters and weeks
            match_map = {'budget_day':["Day",'history_Day'],'history_day':["Day",'history_Day'],
                        'budget_month':['month','history_month'],'history_month':['month','history_month'],
                        'history_week':['week','history_week'],'budget_week':['week','history_week'], 
                        'budget_quarter':['Quarter','history_Quarter'],'history_quarter':['Quarter','history_Quarter']
                        }
            for i,v in zip(keys,values):
                if v == last_slection:
                    if v in match_map:

                        map_list = match_map[v]
                        print(i,v,"iv")
                        print("yes it came here")
                        not_last_selction = set(map_list)-set([i])
                        print(not_last_selction,"no last")

                        SUB_FILTER[list(not_last_selction)[0]] = list(set(data_filter['secondary_filter'][i]).intersection(set(SUB_FILTER[list(not_last_selction)[0]])))
            SUB_FILTER['article_score'] = gloabal_vars.kpi_metrics


        except Exception as e:
            print(traceback.format_exc())
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


    
    def calculate_df(self, data: pl.DataFrame) ->pl.DataFrame:
        """
        Perform operations on the DataFrame after grouping.

        Args:
            data (pl.DataFrame): Input DataFrame containing data for calculations.

        Returns:
            pl.DataFrame: DataFrame with additional calculated columns.
        Note:-these are direct equations,so i'm not putting any comment for this, it won't be looking nice

        """

        #explain analyze

        data = data.with_columns(pl.col("initial_average_retail_price").replace({np.inf:0,-np.inf:0}).fill_nan(0))
        data = data.with_columns(pl.col("final_price").replace({np.inf:0,-np.inf:0}).fill_nan(0))
        data = data.with_columns((pl.col('budget_percent')*100)/pl.col('budget_percent').sum())

        data = data.with_columns(((pl.col("normalised_sales_mix_final_percent")*100)/pl.col("normalised_sales_mix_final_percent").sum()).alias("normalised_sales_mix_final_percent"))
        data = data.with_columns(pl.col("normalised_sales_mix_final_percent").alias("plan_for_net_sales_mix_percent"))
        data = data.with_columns(((pl.col("fcst_net_sales_value")*100)/pl.col("fcst_net_sales_value").sum()).alias("fcst_net_sales_value_sales_mix_percent"))
        data = data.with_columns(((pl.col("fcst_sku_article")*100)/pl.col("fcst_sku_article").sum()).alias("fcst_sku_article_mix_percent"))
        data = data.with_columns(((pl.col("normalised_sku_article_mix_final_percent")*100)/pl.col("normalised_sku_article_mix_final_percent").sum()).alias("normalised_sku_article_mix_final_percent"))
        data = data.with_columns((pl.col("budget_amount")+pl.col("sales_actual")).alias("sales_act_forecast"))
        data = data.with_columns(((pl.col("budget_amount")/pl.col("sales_act_forecast"))*100).alias("act_forecast_vs_budget_percent"))
        data = data.with_columns(((pl.col("sales_act_forecast")/pl.col("net_sales_ly"))*100).alias("act_forecast_vs_ly_percent"))

        data = data.with_columns((pl.col("sales_act_forecast")/pl.col("total_sku_count")).alias("sales_act_or_forecast_sku_article"))
        data = data.with_columns(((pl.col("sales_act_forecast")/pl.col("net_sales_lly"))*100).alias("act_forecast_vs_ppy_percent"))
        data = data.with_columns((pl.col("budget_amount")/pl.col("total_sku_count")).alias("budget_per_sku"))
        data = data.with_columns(((pl.col("budget_amount")/pl.col("net_sales_ly"))*100).alias("budget_vpy"))
        data = data.with_columns(((pl.col("budget_amount")/pl.col("net_sales_lly"))*100).alias("budget_vppy"))
        data = data.with_columns(((pl.col("budget_amount")/pl.col("sales_actual"))*100).alias("budget_vs_act_forecast_percent_ly"))
        data = data.with_columns(((pl.col("budget_amount")/pl.col("initial_average_retail_price")).round()).alias("budget_qty"))
        data = data.with_columns((-((data["adjusted_budget_gross_margin_percent"] * data["budget_amount"]) - (100 * data["budget_amount"]))/100).alias("budget_cost"))
        data = data.with_columns(((pl.col("budget_qty")/pl.col("total_sku_count")).round()).alias("units_per_sku"))
        data = data.with_columns(((((pl.col("net_sales_ly") - pl.col("cost_of_goods_ly"))/pl.col("net_sales_ly")).fill_nan(0))*100).alias("ly_margin_percent"))
        data = data.with_columns(((((pl.col("net_sales_lly") - pl.col("cost_of_goods_lly"))/pl.col("net_sales_lly")).fill_nan(0))*100).alias("lly_margin_percent"))
        data = data.with_columns(((pl.col("sold_article_count_ly")/pl.col("sold_article_count_ly").sum())*100).alias("sku_article_mix_percent"))
        data = data.with_columns((pl.col("sold_article_count_ly")/((pl.col("opening_sku_article_count") + pl.col("closing_sku_article_count"))/2)).alias("sku_article_turn")) 
        data = data.with_columns((pl.col("temp_sold")/pl.col("total_sku_article_buy").sum()).replace({np.inf:0,-np.inf:0}).fill_nan(0).alias("sku_article_efficency"))
        data = data.with_columns(((pl.col("opening_stock") + pl.col("stock_received_qty")) * pl.col('initial_average_retail_price')).alias("purchase_value")) 
        data = data.with_columns((((pl.col("purchase_value")/pl.col("purchase_value").sum())*100).alias("purchase_value_mix_percent")))
        data = data.with_columns(((pl.col("stock_on_hand_qty")/pl.col("stock_on_hand_qty").sum())*100).replace({np.inf:0,-np.inf:0}).fill_nan(0).alias("stock_on_hand_qty_mix_percent"))
        
        data = data.with_columns(current_stock_on_hand_retail_value = pl.col("current_stock_cost_at_retail") * pl.col("stock_on_hand_qty"))
        # data = data.with_columns(units_buy_by_sku = (pl.col("total_purchase_qty")/pl.col("total_sku_article_buy")).replace({np.inf:0,-np.inf:0}).fill_nan(0).round())
        data = data.with_columns(((pl.col("net_sales_ly")/pl.col("net_sales_ly").sum())*100).alias("net_sales_mix_percent"))
        data = data.with_columns((((pl.col("gross_sales_ly") - pl.col("cost_of_goods_ly"))/pl.col("gross_sales_ly"))*100).alias("initial_gp_percent"))
        data = data.with_columns((((pl.col("net_sales_ly") - pl.col("cost_of_goods_ly"))/pl.col("net_sales_ly"))*100).alias("final_gp_percent"))
        data = data.with_columns((((pl.col("gross_sales_ly") - pl.col("net_sales_ly"))/pl.col("gross_sales_ly"))*100).alias("markdown_percent"))
        data = data.with_columns(stock_turn = pl.col("cost_of_goods_ly") / (((pl.col("opening_stock") * (pl.col("current_stock_on_hand_retail_value")/pl.col("stock_on_hand_qty"))) + pl.col("current_stock_on_hand_retail_value"))/2))
        data = data.with_columns(current_stock_cost = (1-(pl.col("initial_gp_percent")/100)) * pl.col("current_stock_cost_at_retail"))

        data = data.with_columns(current_stock_on_hand_cost_value = pl.col("current_stock_cost") * pl.col("stock_on_hand_qty"))
        data = data.with_columns(((pl.col("top_a_class")/pl.col("sold_article_count_ly"))*100).alias("top_80_percent_to_total_articles"))
        data = data.with_columns(((pl.col("tail")/pl.col("sold_article_count_ly"))*100).alias("tail_percent_to_total_articles"))
        data = data.with_columns(((pl.col("top_and_normal")/pl.col("sold_article_count_ly"))*100).alias("top_and_normal_percent_to_total_article"))
        data = data.with_columns(((pl.col("temp_sold")/(pl.col("temp_purchase")+pl.col("temp_sold")))*100).alias("sellthrough_percent"))
        data = data.with_columns(((pl.col("st_greater_than_70")/pl.col("st_greater_than_70").sum())*100).alias("st_greater_than_70_percent_mix"))
        data = data.with_columns((((pl.col("st_greater_than_70")/pl.col("sold_article_count_ly"))*100).replace({np.inf:0,-np.inf:0}).fill_nan(0)).alias("out_of_stock_percent"))

        return data

        
    def second_grouping(self, data: pd.DataFrame,ds:pd.DataFrame,df_channel:bool) -> Tuple[pl.DataFrame,bool,pl.DataFrame]:
   
        #explain analyze
        """
        Perform a secondary grouping on the DataFrame for calculating speed of sales and bottom column for second level of ui/
        channel+itemid

        Args:
            data (pd.DataFrame): Input DataFrame.
            ds (pd.DataFrame): Secondary DataFrame.
            df_channel (bool): Flag to indicate selected level is second level

        Returns:
            Tuple[pl.DataFrame, bool, pl.DataFrame]: Resulting DataFrame, channel flag, and updated Channel DataFrame for bottom column.

        """

        channel_flag = False
        subs_count = 0
        group = []
        print(1)
         # Get columns for secondary grouping
        for col in data.columns:
            if col in self.max_col:
                group.append(col)
        try:
            sub_group = []
            sub_val = []
            cols = []
            vals = []

            # Check if the data DataFrame is not empty
            if not data.is_empty():
                print(12)
                temp = data[0]
            else:
                return data,channel_flag,df_channel

            # Get sub-groups and values for grouping
            for col in group:
                if col not in self.heirarchy:
                    print(13)
                    sub_group.append(col)
                    sub_val.append(temp[col])
                else:
                    cols.append(col)
                    vals.append(temp[col])

            # Reorder the groups
            group = cols+sub_group[::-1]

             # Identify the last filter in the heirarchy
            last_filter = 0
            if len(cols)>0:
                print(14)
                last_filter = self.heirarchy.index(cols[-1])
                if last_filter < len(self.heirarchy)-1:
                    last_filter = self.heirarchy.index(cols[-1]) + 1
                else:
                    last_filter = self.heirarchy.index(cols[-1])
            elif len(sub_group)>0:
                last_filter = 0

            # Check if group is non-empty
            if len(group)>0:
                print(5)
                # Check if the first group is "Channel"
                if group[0] == "Channel":
                    ds = df_channel.clone()

            # Apply filters based on groups and sub-groups
            if len(group)>1:
                filter = None
                for col,val in zip(cols[1:]+sub_group,vals[1:]+sub_val):
                    print(6)
                    if filter is None:
                        if col == "Budget_date":
                            ds = ds.with_columns(pl.col(col).cast(pl.Utf8))
                            filter = ds[col]  == str(val)
                            print(9)
                        else:
                            filter = ds[col]  == val
                            print(8)
                    else:
                        if col == "Budget_date":
                            print(7)
                            ds = ds.with_columns(pl.col(col).cast(pl.Utf8))
                            filter = filter & (ds[col] == str(val))
                        else:
                            print(10)
                            filter = filter & (ds[col] == val)
                            
                # Apply filters to the secondary DataFrame
                if not ds.filter(list(filter)).is_empty():
                    ds = ds.filter(list(filter))

             # Update ds based on group conditions
            if len(group)>0:
                if group[0] == "Channel":
                    print(11)
                    channel_flag = True
                    df_channel = self.calculate_initial_frame(ds,["ITEMID"])
                current_level = group[-1]
  
            else:
                print(12)
                current_level = self.heirarchy[0]

            inner_level = self.heirarchy[last_filter]
            value_array = {}

             # Check the length of group for different scenarios and calculate speed of sales
            if len(group) == 0:
                print(13)
                result = ds["sold_qty_ly"].cast(pl.Float32).max()/ds["sold_qty_ly"].mean() if ds["sold_qty_ly"].mean() not in [0,np.nan] else 0
                data = data.with_columns(speed_of_sales=result)
            elif len(group) == 1:
                value_array = {}
                for unique in ds[current_level].unique():
                    temp = ds.filter(list(ds[current_level]==unique))
                    sub_array = []
                    for uni in temp[inner_level].unique():
                        sub_array.append(temp.filter(list(temp[inner_level] == uni))["sold_qty_ly"].sum())

                    sums = float(sum(sub_array))
                    value_array[unique] = float(max(sub_array))/(sums/len(sub_array)) if sums != 0 and len(sub_array)!=0 else 0
                data = data.with_columns(speed_of_sales = pl.col(group[0]).replace(value_array))

            elif (len(group) == len(sub_group)) or (len(group) == len(sub_group)+1):
                data = data.with_columns(speed_of_sales = pl.lit(0))
            else:
                uniques = ds[current_level].unique()
                for unique in uniques:
                    temp = ds.filter(list(ds[current_level]==unique))
                    inner_unique = temp[inner_level].unique()
                    means = 0
                    maxs = 0
                    for i_unique in inner_unique:
                        break
                        means = temp.filter(list(temp[inner_level] == i_unique))["sold_qty_ly"].mean()
                        means = np.where(means  == np.nan,0,means)
                        maxs = temp.filter(list(temp[inner_level] == i_unique))["sold_qty_ly"].max()
                        value_array[i_unique] = float(maxs)/means if means != 0 else 0
                data = data.with_columns(speed_of_sales = pl.col(group[0]).replace(value_array))

        except:
            print(traceback.format_exc())
        return data,channel_flag,df_channel 




    def apply_secondary_filters(self,data: pl.DataFrame,filter_data: Filters,sub_filter_state:bool,
                group:List,filter_condition:[Optional]=None) ->Union[None,pl.Series,bool,List]:
        '''
        Apply secondary filters to the data based on the provided filter_data.

        Args:
            data (pl.DataFrame): DataFrame to apply filters.
            filter_data (Filters): Filters containing secondary filter values.
            sub_filter_state (bool): State of sub-filters.
            group (List): List of selected group columns/sub filter columns.
            filter_condition (Optional): Existing filter condition.

        Returns:
            Union[None, pl.Series, bool, List]: Updated filter condition, sub_filter_state, and group.

        '''

        # Mapping between filter_data keys and column names
        key_map = {'Store_Name':'Store','region':'Region','Channel':'Channel','BudgetYear':'Budget_Year','Quarter':'budget_quarter',
        'month':'budget_month','week':'budget_week','Day':'budget_day','BudgetDate':'Budget_date','HistoricalYear':'historical_year','history_Quarter':'history_quarter',
        'history_month':'history_month','history_week':'history_week','history_Day':'history_day','history_dates':"History_date",'article_score':gloabal_vars.SCORES
        }
    
        try:
            # Iterate over filter_data keys and values
            for key,values in filter_data.secondary_filter.dict().items():
                
                    # Skip if values are empty
                    if not values or key =='article_score':
                        continue
                    sub_filter_state = True
                    # obtaind filter condition based on the key and values
                    if key_map[key] in ['Budget_Year']:
                        
                        values = [int(i) for i in values]
                        new_condition = data[key_map[key]].is_in(values)
                    else:
                        new_condition = data[key_map[key]].is_in(values)
                    group.append(key_map[key])

                    if filter_condition  is None:
                        filter_condition = new_condition
                    else:
                        filter_condition = filter_condition & new_condition
        except Exception as e:
            print(traceback.format_exc())
            print(f"Error applyting filters:{e}")
        
        # If sub_filter_state is not True, reset group, filter_condition, and sub_filter_state
        if not sub_filter_state == True:
            group = []
            filter_condition = None
            sub_filter_state = False    
        return filter_condition,sub_filter_state,group
    

    def drill_down(self,df:pl.DataFrame,columnID:str,newValue:float,ref_col:str) ->pl.DataFrame:
        """
        Perform drill down on the DataFrame.

        Args:
            df (pl.DataFrame): Input DataFrame.
            columnID (str): Column to be modified.
            newValue (float): New value to be assigned.
            ref_col (str): Reference column for drill down.

        Returns:
            pl.DataFrame: Resulting DataFrame after drill down.

        """
        if columnID == "plan_for_otb_value":
            # Update the specified column with calculated values based on the percentage of ref_col and budget_amount
            df = df.with_columns((((df[ref_col]+df["budget_amount"])/(df[ref_col].sum()+df["budget_amount"].sum())) * newValue).alias(columnID))
        elif columnID == "plan_for_sku_article_count":
            # Update the specified column with calculated values based on the percentage of ref_col and total_sku_count
            df = df.with_columns((((df[ref_col]+df["total_sku_count"])/(df[ref_col].sum()+df["total_sku_count"].sum())) * newValue).alias(columnID))
        else:
            # Update the specified column with calculated values based on the percentage of ref_col
            df = df.with_columns(((df[ref_col]/df[ref_col].sum()) * newValue).alias(columnID))
        return df

    def drill_down_percentage(self,df:pl.DataFrame,columnID:str,newValue:float,ref_col:str) ->pl.DataFrame:
        """
        Perform drill down on the DataFrame based on a percentage using mean()

        Args:
            df (pl.DataFrame): Input DataFrame.
            columnID (str): Column to be modified.
            newValue (float): New value to be assigned.
            ref_col (str): Reference column for drill down.

        Returns:
            pl.DataFrame: Resulting DataFrame after drill down.

        """
        df = df.with_columns(((df[ref_col]/df[ref_col].mean()) * newValue).alias(columnID))
        return df


    def drill_down_multi(self,df:pl.DataFrame,columnID:str,newValue:float,ref_col:List) -> pl.DataFrame:
        """
        Perform multi-level drill down on the DataFrame.

        Args:
            df (pl.DataFrame): Input DataFrame.
            columnID (str): Column to be modified.
            newValue (float): New value to be assigned.
            ref_col (List): Reference columns to performe drill down operations.

        Returns:
            pl.DataFrame: Resulting DataFrame after multi-level drill down.

        """
        df_temp = pl.DataFrame({'distributor':[0]*len(df)})
        # Aggregate values from reference columns
        for col in ref_col:
            df_temp = df_temp.with_columns((df_temp['distributor'] + df[col]).alias('distributor'))
        df = df.with_columns(((df_temp['distributor']/(df_temp['distributor'].sum())) * newValue).alias(columnID))

        return df
    


    def calculate_initial_frame(self,polars_df:pl.DataFrame,subset:List) ->pl.DataFrame:
 
        """
        Perform operations on the input DataFrame before grouping.

        Args:
            polars_df (pl.DataFrame): Input DataFrame containing data for calculations.
            subset (List): List of columns to consider during the operations.

        Returns:
            pl.DataFrame: DataFrame after calculation.

        """

        # Calculate the total_sku_count based on the distinct values in the specified subset
        polars_df= polars_df.with_columns(
            (pl.struct(subset).is_first_distinct().cast(pl.Int8).alias("total_sku_count"))
            )
        # Calculate temp_sold as the sum of sold_qty_ly over the specified subset multiplied by total_sku_count
        polars_df= polars_df.with_columns(
                ((pl.col("sold_qty_ly").sum().over(subset)) * pl.col("total_sku_count")).alias("temp_sold").cast(pl.Float32))

        quantity_set = set(subset).intersection(set(gloabal_vars.time_columns)) 



        sales_set = ["ITEMID", "Channel", "INVENTLOCATIONID",'history_day']
        #  
        # df = df.with_columns((pl.col("budget_amount")/pl.col("budget_amount").count().over(subset)).alias("budget_amount"))
        polars_df = polars_df.with_columns(net_sales_ly = (pl.col("net_sales_ly")/pl.col("net_sales_ly").count().over(sales_set)).fill_nan(0.0).replace({np.inf:0.0,-np.inf:0.0}))
        polars_df = polars_df.with_columns(gross_sales_ly = (pl.col("gross_sales_ly")/pl.col("gross_sales_ly").count().over(sales_set)).fill_nan(0.0).replace({np.inf:0.0,-np.inf:0.0}))
        polars_df = polars_df.with_columns(cost_of_goods_ly = (pl.col("cost_of_goods_ly")/pl.col("cost_of_goods_ly").count().over(sales_set)).fill_nan(0.0).replace({np.inf:0.0,-np.inf:0.0}))
        polars_df = polars_df.with_columns(sold_qty_ly = (pl.col("sold_qty_ly")/pl.col("sold_qty_ly").count().over(sales_set)).fill_nan(0.0).replace({np.inf:0.0,-np.inf:0.0}))
        
        polars_df = polars_df.with_columns(net_sales_lly = (pl.col("net_sales_lly")/pl.col("net_sales_lly").count().over(sales_set)).fill_nan(0.0).replace({np.inf:0.0,-np.inf:0.0}))
        polars_df = polars_df.with_columns(cost_of_goods_lly = (pl.col("cost_of_goods_lly")/pl.col("cost_of_goods_lly").count().over(sales_set)).fill_nan(0.0).replace({np.inf:0.0,-np.inf:0.0}))

 
        # calculate budget/qty or unit buy by sku 
        polars_df= polars_df.with_columns(
                budget_qty = (((pl.col("budget_amount").sum()/pl.col("initial_average_retail_price").mean()).over(quantity_set)).replace({np.inf:0,-np.inf:0}).round())*pl.col("total_sku_count")
                )

        # Calculate temp_purchase as the sum of stock_on_hand_qty over the specified subset multiplied by total_sku_count
        polars_df= polars_df.with_columns(
                ((pl.col("stock_on_hand_qty").sum().over(subset))* pl.col("total_sku_count")).alias("temp_purchase").cast(pl.Float32)
                )
        # Calculate sellthrough_percent as the percentage of temp_sold to the sum of temp_purchase and temp_sold
        polars_df= polars_df.with_columns(
                (pl.col("temp_sold")/(pl.col("temp_purchase")+pl.col("temp_sold"))*100).alias("sellthrough_percent").fill_nan(0))
        # Identify if sellthrough_percent is greater than 70 and cast the result to Int8
        polars_df= polars_df.with_columns(
                (pl.col("sellthrough_percent").fill_nan(0)>70).cast(pl.Int8).alias("st_greater_than_70")
                )
        # Identify if total_purchase_qty.sum() over the subset multiplied by total_sku_count is greater than 0 then 1 else 0
        polars_df= polars_df.with_columns(
                (pl.col("total_purchase_qty").sum().over(subset) * pl.col("total_sku_count")).gt(0).cast(pl.Int8).alias("total_sku_article_buy")
                )
         # Identify if opening_stock.sum() over the subset multiplied by total_sku_count is greater than 0 then 1 else 0
        polars_df= polars_df.with_columns(
                (pl.col("opening_stock").sum().over(subset) * pl.col("total_sku_count")).gt(0).cast(pl.Int8).alias("opening_sku_article_count")
                )
        # Identify if closing_stock.sum() over the subset multiplied by total_sku_count is greater than 0 then 1 else 0
        polars_df= polars_df.with_columns(
                (pl.col("closing_stock").sum().over(subset) * pl.col("total_sku_count")).gt(0).cast(pl.Int8).alias("closing_sku_article_count")
                )
        # Identify if sold_qty_ly.sum() over the subset multiplied by total_sku_count is greater than 0 then 1 else 0
        polars_df= polars_df.with_columns(
                (pl.col("sold_qty_ly").sum().over(subset) * pl.col("total_sku_count")).gt(0).cast(pl.Int8).alias("sold_article_count_ly")
                )

        # indidual sum of item
        polars_df= polars_df.with_columns(
                (pl.col('net_sales_ly').sum().over(subset)).alias('group_sums')
                )

        # Calculate cum_sum as the cumulative sum of net_sales_ly.sum() over the subset multiplied by sold_article_count_ly
        polars_df= polars_df.with_columns(
                (pl.col('net_sales_ly').sum().over(subset) * pl.col("sold_article_count_ly")).alias('cum_sum')
                )
        # Calculate cumulative sum
        polars_df = polars_df.sort(by='cum_sum',descending =True)
        polars_df = polars_df.with_columns((pl.cumsum('cum_sum')* pl.col("sold_article_count_ly")).alias("cum_sum"))
        sums = polars_df["cum_sum"].max() * 0.8
        sums_2 = polars_df["cum_sum"].max() * 0.95

        # Identify top_a_class as 1 if cum_sum is less than or equal to 80% of the maximum cum_sum, otherwise 0
        polars_df = polars_df.with_columns((pl.when(((pl.col("cum_sum") <= sums) & (pl.col("cum_sum")>0))|(pl.col('group_sums')>sums))
                                        .then(1)
                                        .otherwise(0)).alias("top_a_class"))
        # Identify tail as 1 if cum_sum is greater than 95% of the maximum cum_sum, otherwise 0
        polars_df = polars_df.with_columns((pl.when(pl.col("cum_sum") > sums_2)
                                .then(1)
                                .otherwise(0)).alias("tail"))
        # Identify top_and_normal as 1 if cum_sum is less than or equal to 95% of the maximum cum_sum and greater than 0
        polars_df = polars_df.with_columns((pl.when(((pl.col("cum_sum") <= sums_2) & (pl.col("cum_sum")>0))|(pl.col('group_sums')>sums))
                                .then(1)
                                .otherwise(0)).alias("top_and_normal"))
  
        return polars_df

  




    def change_percent(self,grouped_df:pl.DataFrame,other_grouped_df:pl.DataFrame,increase:float,colID:str) -> pl.DataFrame:
        '''
        Change the values in the specified column (colID) of grouped_df and other_grouped_df based on the increase percentage.

        Args:
            grouped_df (pl.DataFrame): DataFrame to be modified.
            other_grouped_df (pl.DataFrame): Another DataFrame for the same modification.
            increase (float): Percentage increase factor.
            colID (str): Name of the column to be modified.

        Returns:
            pl.DataFrame: Concatenated DataFrame with modified values.

        '''
        summation = grouped_df[colID].fill_nan(0).sum()
        # Increase the values in the specified column of grouped_df
        grouped_df = grouped_df.with_columns((grouped_df[colID] + (grouped_df[colID]*increase)/summation).alias(colID))
        unsummation = other_grouped_df[colID].fill_nan(0).sum()
        # Decrease the values in the specified column of other_grouped_df
        other_grouped_df = other_grouped_df.with_columns((other_grouped_df[colID] - (other_grouped_df[colID] * increase) / unsummation).alias(colID))
        frames =  [grouped_df,other_grouped_df]
        df = pl.concat(frames)
        return df

    def drill_input_editable(self,grouped_df:pl.DataFrame,other_grouped_df:pl.DataFrame,increase:float,colID:str,ref_col:str) ->pl.DataFrame:
        """
        Distribute values to columns from top to bottom using the reference column.

        Args:
            grouped_df (pl.DataFrame): DataFrame to be modified.
            other_grouped_df (pl.DataFrame): Another DataFrame for the same modification.
            increase (float): Percentage increase factor.
            colID (str): Name of the column to be modified.
            ref_col (str): Name of the reference column.

        Returns:
            pl.DataFrame: Concatenated DataFrame with modified values.

        """

        summation = grouped_df[ref_col].sum()+grouped_df[colID].sum()
        # Distribute values in the specified column of grouped_df from top to bottom using the reference column
        grouped_df = grouped_df.with_columns(((grouped_df[colID]+(((grouped_df[ref_col]+grouped_df[colID])*increase)/(summation))).fill_nan(0)).alias(colID))
        unsummation = other_grouped_df[ref_col].sum()+other_grouped_df[colID].sum()
        # Distribute values in the specified column of other_grouped_df from top to bottom using the reference column
        other_grouped_df = other_grouped_df.with_columns(((other_grouped_df[colID]- (((other_grouped_df[ref_col]+other_grouped_df[colID])*increase)/(unsummation))).fill_nan(0)).alias(colID))
        frames =  [grouped_df,other_grouped_df]
        df = pl.concat(frames)

        return df



    def calculate_quantity(self, data: pl.DataFrame) ->pl.DataFrame:
        """
        Calculate various quantity-related metrics.

        Args:
            data (pl.DataFrame): Input DataFrame.

        Returns:
            pl.DataFrame: DataFrame with calculated quantity-related metrics.

        """

        data = data.with_columns((((pl.col("budget_amount")/pl.col("initial_average_retail_price"))*100).replace({np.inf:0,-np.inf:0}).fill_nan(0)).alias("budget_qty"))
        data = data.with_columns(((pl.col("budget_qty")*100)/(pl.col("budget_qty").sum())).alias("budget_qty_percent"))
        data = data.with_columns((pl.col("budget_qty")+pl.col("quantity_actuals")).alias("quantity_act_forecast"))
        data = data.with_columns(((pl.col("budget_qty")/pl.col("quantity_act_forecast")).replace({np.inf:0,-np.inf:0}).fill_nan(0)*100).alias("quantity_act_forecast_vs_budget_percent"))
        data = data.with_columns(((pl.col("quantity_act_forecast")/pl.col("sold_qty_ly")).replace({np.inf:0,-np.inf:0}).fill_nan(0)*100).alias("quantity_act_forecast_vs_ly_percent"))
        data = data.with_columns(((pl.col("quantity_act_forecast")/pl.col("total_sku_count")).replace({np.inf:0,-np.inf:0}).fill_nan(0)*100).alias("quantity_act_or_forecast_per_sku"))
        data = data.with_columns(((pl.col("quantity_act_forecast")/pl.col("quantity_ppy")).replace({np.inf:0,-np.inf:0}).fill_nan(0)*100).alias("quantity_act_forecast_vs_ppy_percent"))
        data = data.with_columns((pl.col("budget_qty")/pl.col("total_sku_count")).alias("budget_per_sku_qty"))
        data = data.with_columns(((pl.col("budget_qty")/pl.col("sold_qty_ly"))*100).alias("budget_vs_py_qty_percent"))
        data = data.with_columns(((pl.col("budget_qty")/pl.col("quantity_ppy")).fill_nan(0)*100).alias("budget_vs_ppy_qty_percent"))
        data = data.with_columns(((pl.col("sold_qty_ly")*100)/(pl.col("sold_qty_ly").sum())).alias("quantity_mix_percent"))
        return data
        

    def calculate_cost(self, data: pl.DataFrame) ->pl.DataFrame:
            """
            Calculate various cost-related metrics.

            Args:
                data (pl.DataFrame): Input DataFrame.

            Returns:
                pl.DataFrame: DataFrame with calculated cost-related metrics.

            """

            data = data.with_columns((((pl.col("budget_cost")*100)/(pl.col("budget_cost").sum())).replace({np.inf:0,-np.inf:0}).fill_nan(0)).alias("budget_cost_percent"))
            data = data.with_columns((pl.col("budget_cost")+pl.col("cost_actuals")).alias("cost_act_forecast"))
            data = data.with_columns((((pl.col("budget_cost")/pl.col("cost_act_forecast"))*100).replace({np.inf:0,-np.inf:0}).fill_nan(0)).alias("budget_vs_act_forecast_cost_percent"))
            data = data.with_columns((((pl.col("cost_act_forecast") /pl.col("cost_of_goods_ly"))*100).replace({np.inf:0,-np.inf:0}).fill_nan(0)).alias("cost_act_forecast_vs_ly_percent"))
            data = data.with_columns((((pl.col("cost_act_forecast") /pl.col("total_sku_count"))*100).replace({np.inf:0,-np.inf:0}).fill_nan(0)).alias("cost_act_or_forecast_per_sku"))
            data = data.with_columns((((pl.col("cost_act_forecast") /pl.col("cost_of_goods_lly"))*100).replace({np.inf:0,-np.inf:0}).fill_nan(0)).alias("cost_act_forecast_vs_ppy_percent"))
            data = data.with_columns(((pl.col("budget_cost")/pl.col("total_sku_count")).replace({np.inf:0,-np.inf:0}).fill_nan(0)).alias("cost_budget_per_sku"))
            data = data.with_columns((((pl.col("budget_cost")/pl.col("cost_of_goods_ly"))*100).replace({np.inf:0,-np.inf:0}).fill_nan(0)).alias("budget_vs_py_cost_percent"))
            data = data.with_columns((((pl.col("budget_cost")/pl.col("cost_of_goods_lly"))*100).replace({np.inf:0,-np.inf:0}).fill_nan(0)).alias("budget_vs_ppy_cost_percent"))
            data = data.with_columns((((pl.col("cost_of_goods_ly")*100)/(pl.col("cost_of_goods_ly").sum())).replace({np.inf:0,-np.inf:0}).fill_nan(0)).alias("cost_mix_percent"))
            return data
            


    def calculate_margin(self, data: pl.DataFrame) ->pl.DataFrame:
    
            """
            Calculate various margin-related metrics.

            Args:
                data (pl.DataFrame): Input DataFrame.

            Returns:
                pl.DataFrame: DataFrame with calculated margin-related metrics. 

            """
            data = data.with_columns((((pl.col("sales_actual") - pl.col("cost_actuals"))/pl.col("sales_actual")).replace({np.inf:0,-np.inf:0}).fill_nan(0)).alias("margin_actuals"))
            data = data.with_columns(((((pl.col("net_sales_ly") - pl.col("cost_of_goods_ly"))/pl.col("net_sales_ly"))*100).replace({np.inf:0,-np.inf:0}).fill_nan(0)).alias("ly_margin_percent"))
            data = data.with_columns(((((pl.col("net_sales_lly") - pl.col("cost_of_goods_lly"))/pl.col("net_sales_ly"))*100).replace({np.inf:0,-np.inf:0}).fill_nan(0)).alias("lly_margin_percent"))
            data = data.with_columns(budget_gross_margin = pl.col("budget_gross_margin_percent")/100)
            data = data.with_columns((((pl.col("budget_gross_margin")*100)/(pl.col("budget_gross_margin").sum())).replace({np.inf:0,-np.inf:0}).fill_nan(0)).alias("budget_margin_mix_percent"))
            data = data.with_columns((((pl.col("budget_gross_margin")/pl.col("budget_amount"))*100).replace({np.inf:0,-np.inf:0}).fill_nan(0)).alias("budget_margin_percent"))
            data = data.with_columns(margin_act_forecast = pl.col("budget_gross_margin").fill_nan(0)+pl.col("margin_actuals").fill_nan(0))
            data = data.with_columns((((pl.col("budget_gross_margin")/pl.col("margin_act_forecast"))*100).replace({np.inf:0,-np.inf:0}).fill_nan(0)).alias("budget_vs_act_forecast_margin_percent"))
            data = data.with_columns((((pl.col("margin_act_forecast") /(pl.col("ly_margin_percent")*100))*100).replace({np.inf:0,-np.inf:0}).fill_nan(0)).alias("margin_act_forecast_vs_ly_percent"))
            data = data.with_columns((((pl.col("margin_act_forecast") /pl.col("total_sku_count"))*100).replace({np.inf:0,-np.inf:0}).fill_nan(0)).alias("margin_act_or_forecast_per_sku"))
            data = data.with_columns((((pl.col("margin_act_forecast") /(pl.col("lly_margin_percent")*100))*100).replace({np.inf:0,-np.inf:0}).fill_nan(0)).alias("margin_act_forecast_vs_ppy_percent"))
            data = data.with_columns(((pl.col("budget_gross_margin")/pl.col("total_sku_count")).replace({np.inf:0,-np.inf:0}).fill_nan(0)).alias("margin_budget_per_sku"))
            data = data.with_columns((((pl.col("budget_gross_margin")/(pl.col("ly_margin_percent")*100))*100).replace({np.inf:0,-np.inf:0}).fill_nan(0)).alias("budget_vs_py_margin_percent"))
            data = data.with_columns((((pl.col("budget_gross_margin")/(pl.col("lly_margin_percent")*100))*100).replace({np.inf:0,-np.inf:0}).fill_nan(0)).alias("budget_vs_ppy_margin_percent"))
            data = data.with_columns((((pl.col("margin_actuals")*100)/(pl.col("margin_actuals").sum())).replace({np.inf:0,-np.inf:0}).fill_nan(0)).alias("margin_mix_percent"))
            return data






    def save_to_db(self,df:pl.DataFrame,table_name:str,db:Session):
        # ra_columns = {'BudgetAmount':'budget_amount','BudgetDate':'Budget_date',
        # 'BudgetYear':'Budget_Year','Budget_GrossMargin%':'budget_gross_margin_percent',
        # 'CategoryName':'Category','Day':'budget_day','Item_Code':'ITEMID',
        # 'Item_Cost':'budget_cost','Quantity':'budget_qty','Quarter':'budget_quarter',
        # 'Store_Name':'Store','month':'budget_month','region':'Region',
        # 'subFamily':'SubFamily','week':'budget_week'}
        print(round(df.estimated_size('gb'),2)," GB memory size of data")


        max_icols = ['Store','area','Region']
        avg_icols = ["adjusted_budget_gross_margin_percent"]
        sumi_cols = [ 'budget_amount','budget_cost','budget_qty',]
        subset = ['ITEMID','Channel','INVENTLOCATIONID','Budget_date']


        # agg_dict = [eval(expr) for expr in [f"pl.col('{col}').mean()" for col in avg_icols ]
        #             + [f"pl.col('{col}').sum()" for col in sumi_cols] +
        #              [f"pl.col('{col}').max()" for col in max_icols]]

        print(round(df.estimated_size('gb'),2)," GB memory size of data")

        # df = df.group_by(subset).agg(agg_dict)
        df = df.to_pandas()  
        print(df.columns.to_list(), 'columns final save')
        df = df[max_icols+avg_icols+sumi_cols+subset]
        # df.head(0).to_sql(table_name, con=db.bind, if_exists='replace', index=False)

        df = df.rename(columns={'budget_qty':"BudgetQTY","adjusted_budget_gross_margin_percent":'budget_gross_margin_percent'})
        print(f"{ctime()} - csv conversion begins")
        df.to_csv(f"{table_name}.tsv",header=True,index=False,sep='\t')
        print(f"{ctime()} - csv conversion ends")
        print(f"{ctime()} - writing data to db begins")

        with db.bind.raw_connection().cursor() as cursor:
            try:
                cursor.execute(f"TRUNCATE TABLE {table_name};")
                with open(f'{table_name}.tsv', 'r',) as file:
                    cursor.execute("BEGIN;")
                    cursor.copy_expert(sql = f"COPY {table_name} FROM STDIN WITH DELIMITER E'\t' CSV HEADER;",file=file)
                cursor.execute("COMMIT;")
            except:
                print(traceback.format_exc())
                cursor.connection.rollback()
            finally:
                cursor.close()
                os.remove(f'{table_name}.tsv')
        print(f"{ctime()} - writing data to db ends")
  

       

    def calculate_article_count(self,df:pl.DataFrame,percent:float,col:str,ref_col:str) -> pl.DataFrame:
        """
        Calculate the top items based on a percentage of a specified column.

        Args:
            df (pl.DataFrame): Input DataFrame.
            percent (float): Percentage value to determine the top items.
            col (str): Column used to calculate the length from the percentage.
            ref_col (str): Reference column for sorting and selecting top items.

        Returns:
            pl.DataFrame: DataFrame with the top items.

        """
        selected_counts = int(percent*df[col].sum())
        df = df.with_columns((df[ref_col]* df[col]).alias(ref_col))
        df = df.sort(by=ref_col,descending=True)
        df = df.head(selected_counts)
        return df[["ITEMID"]]
    
    def calculate_bottom_first_tab(self,data:Dict) ->Dict:
        """
        Calculate various metrics for the first tab of the bottom part.

        Args:
            data (Dict): Input data containing various metrics.

        Returns:
            Dict: bottom column with Calculated metrics 

        """
        data["budget_per_sku"] = data["budget_amount"]/data["total_sku_count"] if data["total_sku_count"] !=0 else 0
        data["Budget vs PY%"] = (data["budget_amount"]/data["net_sales_ly"])*100 if data["net_sales_ly"] !=0 else 0
        data["Budget vs PPY%"] = (data["budget_amount"]/data["net_sales_lly"])*100 if data["net_sales_lly"] !=0 else 0
        data["units_per_sku"] = data["budget_qty"]/data["total_sku_count"] if data["total_sku_count"] !=0 else 0
        data["initial_gp_percent"] = ((data["gross_sales_ly"] - data["cost_of_goods_ly"])/data["gross_sales_ly"])*100 if data["gross_sales_ly"] !=0 else 0
        data["final_gp_percent"] = (((data["net_sales_ly"] - data["cost_of_goods_ly"])/data["net_sales_ly"]))*100 if data["net_sales_ly"] !=0 else 0
        data["markdown_percent"] = ((data["gross_sales_ly"]-data["net_sales_ly"])/(data["gross_sales_ly"]))*100 if data["gross_sales_ly"] !=0 else 0
        bottom_turn = (((float(data["opening_stock"])*(data["current_stock_on_hand_retail_value"]/float(data["stock_on_hand_qty"])))+data["current_stock_on_hand_retail_value"])/2) if data["stock_on_hand_qty"] !=0 else 0
        data["stock_turn"] = data["cost_of_goods_ly"]/bottom_turn if bottom_turn !=0 else 0
        bottom_turn = ((data["opening_sku_article_count"]+data["closing_sku_article_count"])/2)
        data["sku_article_turn"] = data["sold_article_count_ly"]/bottom_turn if bottom_turn !=0 else 0
        data["sku_article_efficency"] = float(data["temp_sold"])/data["total_sku_article_buy"] if data["total_sku_article_buy"] !=0 else 0
        data["sales_act_forecast"] = data["budget_amount"]+data["sales_actual"]
        data["act_forecast_vs_budget_percent"] = (data["budget_amount"]/data["sales_act_forecast"])*100 if data["sales_act_forecast"] !=0 else 0
        data["act_forecast_vs_ly_percent"] = (data["sales_act_forecast"] /data["net_sales_ly"])*100 if data["net_sales_ly"] !=0 else 0
        data["sales_act_or_forecast_sku_article"] = (data["sales_act_forecast"] /data["total_sku_count"]) if data["total_sku_count"] !=0 else 0
        data["act_forecast_vs_ppy_percent"] = (data["sales_act_forecast"] /data["net_sales_lly"])*100 if data["net_sales_lly"] !=0 else 0
        data["ly_margin_percent"] = (((data["net_sales_ly"]- data["cost_of_goods_ly"])/data["net_sales_ly"]))*100 if data["net_sales_ly"] !=0 else 0
        data["lly_margin_percent"] = (((data["net_sales_lly"]- data["cost_of_goods_lly"])/data["net_sales_lly"]))*100 if data["net_sales_lly"] !=0 else 0
        data["purchase_value"] = (data["opening_stock"] + data["stock_received_qty"])*data['initial_average_retail_price']
        data["current_stock_cost"] = (1-(data["initial_gp_percent"]/100))* data["current_stock_cost_at_retail"] 
        data["current_stock_on_hand_cost_value"] = data["current_stock_cost"] * data["stock_on_hand_qty"]
        data["top_80_percent_to_total_articles"] = (data["top_a_class"]/data["sold_article_count_ly"])*100 if data["sold_article_count_ly"] !=0 else 0
        data["tail_percent_to_total_articles"] = (data["tail"]/data["sold_article_count_ly"])*100 if data["sold_article_count_ly"] !=0 else 0
        data["top_and_normal_percent_to_total_article"] = (data["top_and_normal"]/data["sold_article_count_ly"])*100 if data["sold_article_count_ly"] !=0 else 0
        data["sellthrough_percent"] = (data["temp_sold"]/(data["temp_purchase"]+data["temp_sold"]))*100 if (data["temp_purchase"]+data["temp_sold"])  !=0 else 0
        data["out_of_stock_percent"] = (data["st_greater_than_70"]/data["sold_article_count_ly"])*100 if data["sold_article_count_ly"] !=0 else 0


        return data

        
    def calculate_bottom_qty_tab(self,data:Dict) ->Dict:
        """
        Calculate various metrics for the quantity tab of the bottom part.

        Args:
            data (Dict): Input data containing various metrics.
            
        Returns:
            Dict: bottom column with Calculated metrics 

        """
        data["quantity_act_forecast_vs_budget_percent"] = (data["budget_qty"]/data["quantity_act_forecast"])*100 if data["quantity_act_forecast"] !=0 else 0
        data["quantity_act_or_forecast_per_sku"] = data["quantity_act_forecast"]/data["total_sku_count"] if data["total_sku_count"] !=0 else 0
        data["quantity_act_forecast_vs_ly_percent"] = (data["quantity_act_forecast"]/data["sold_qty_ly"])*100 if data["sold_qty_ly"] !=0 else 0
        data["quantity_act_forecast_vs_ppy_percent"] = (data["quantity_act_forecast"]/data["quantity_ppy"])*100 if data["quantity_ppy"] !=0 else 0
        data["budget_per_sku_qty"] = data["budget_qty"]/data["total_sku_count"] if data["total_sku_count"] !=0 else 0
        data["budget_vs_py_qty_percent"] = (data["budget_qty"]/data["sold_qty_ly"])*100 if data["sold_qty_ly"] !=0 else 0
        data["budget_vs_ppy_qty_percent"] = (data["budget_qty"]/data["quantity_ppy"])*100 if data["quantity_ppy"] !=0 else 0
        return data


    def calculate_bottom_cost_tab(self,data:Dict) ->Dict:
        """
        Calculate various metrics for the cost tab of the bottom part.

        Args:
            data (Dict): Input data containing various metrics.
            
        Returns:
            Dict: bottom column with Calculated metrics 

        """
        data["cost_act_forecast"] = data["budget_cost"]+data["cost_actuals"]
        data["budget_vs_act_forecast_cost_percent"] = (data["budget_cost"]/data["cost_act_forecast"])*100 if data["cost_act_forecast"] !=0 else 0
        data["cost_act_forecast_vs_ly_percent"] = (data["cost_act_forecast"] /data["cost_of_goods_ly"])*100 if data["cost_of_goods_ly"] !=0 else 0
        data["cost_act_or_forecast_per_sku"] = (data["cost_act_forecast"] /data["total_sku_count"])*100 if data["total_sku_count"] !=0 else 0
        data["cost_act_forecast_vs_ppy_percent"] = (data["cost_act_forecast"] /data["cost_of_goods_lly"])*100 if data["cost_of_goods_lly"] !=0 else 0
        data["cost_budget_per_sku"] = data["budget_cost"]/data["total_sku_count"] if data["total_sku_count"] !=0 else 0
        data["budget_vs_py_cost_percent"] = (data["budget_cost"]/data["cost_of_goods_ly"])*100 if data["cost_of_goods_ly"] !=0 else 0
        data["budget_vs_ppy_cost_percent"] = (data["budget_cost"]/data["cost_of_goods_lly"])*100 if data["cost_of_goods_lly"] !=0 else 0
        return data

    def calculate_bottom_margin_tab(self,data:Dict) ->Dict:
        """
        Calculate various metrics for the margin tab of the bottom part.

        Args:
            data (Dict): Input data containing various metrics.
            
        Returns:
            Dict: bottom column with Calculated metrics 

        """
        data["margin_actuals"] = (data["sales_actual"] - data["cost_actuals"])/data["sales_actual"] if data["sales_actual"] !=0 else 0
        data["budget_margin_percent"] = (data["budget_gross_margin"]/data["budget_amount"])*100 if data["budget_amount"] !=0 else 0
        data["margin_act_forecast"] = data["budget_gross_margin"]+data["margin_actuals"]
        data["budget_vs_act_forecast_margin_percent"] = (data["budget_gross_margin"]/data["margin_act_forecast"])*100 if data["margin_act_forecast"] !=0 else 0
        data["margin_act_forecast_vs_ly_percent"] = (data["margin_act_forecast"] / data["ly_margin_percent"])*100 if data["ly_margin_percent"] !=0 else 0
        data["margin_act_or_forecast_per_sku"] = (data["margin_act_forecast"] /data["total_sku_count"])*100 if data["total_sku_count"] !=0 else 0
        data["margin_act_forecast_vs_ppy_percent"] = (data["margin_act_forecast"] /data["lly_margin_percent"])*100 if data["lly_margin_percent"] !=0 else 0
        data["margin_budget_per_sku"] = data["budget_gross_margin"]/data["total_sku_count"] if data["total_sku_count"] !=0 else 0
        data["budget_vs_py_margin_percent"] = (data["budget_gross_margin"]/data["ly_margin_percent"])*100 if data["ly_margin_percent"] !=0 else 0
        data["budget_vs_ppy_margin_percent"] = (data["budget_gross_margin"]/data["lly_margin_percent"])*100 if data["lly_margin_percent"] !=0 else 0
        return data

    def calculate_kpi_final(self,data:pl.DataFrame) ->pl.DataFrame:
        """
        Calculate various Key Performance Indicators (KPIs) for given dataframe.

        Args:
            data (pl.DataFrame): Input dataframe containing various metrics.

        Returns:
            pl.DataFrame: Calculated KPIs added to the input data frame.

        """
        data = data.with_columns(pl.col("initial_average_retail_price").replace({np.inf:0,-np.inf:0}).fill_nan(0))
        data = data.with_columns(pl.col("final_price").replace({np.inf:0,-np.inf:0}).fill_nan(0))
        data = data.with_columns(((pl.col("opening_stock") + pl.col("stock_received_qty")) * pl.col('initial_average_retail_price')).alias("purchase_value")) 
        data = data.with_columns((((pl.col("purchase_value")/pl.col("purchase_value").sum())*100).alias("purchase_value_mix_percent")))
        data = data.with_columns(((pl.col("net_sales_ly")/pl.col("net_sales_ly").sum())*100).alias("net_sales_mix_percent"))
        data = data.with_columns((((pl.col("gross_sales_ly") - pl.col("cost_of_goods_ly"))/pl.col("gross_sales_ly"))*100).alias("initial_gp_percent"))
        data = data.with_columns(((pl.col("top_a_class")/pl.col("sold_article_count_ly"))*100).alias("top_80_percent_to_total_articles"))
        data = data.with_columns(((pl.col("tail")/pl.col("sold_article_count_ly"))*100).alias("tail_percent_to_total_articles"))
        data = data.with_columns(((pl.col("top_and_normal")/pl.col("sold_article_count_ly"))*100).alias("top_and_normal_percent_to_total_article"))
        data = data.with_columns(((pl.col("temp_sold")/(pl.col("temp_purchase")+pl.col("temp_sold")))*100).alias("sellthrough_percent"))
        data = data.with_columns(((pl.col("st_greater_than_70")/pl.col("st_greater_than_70").sum())*100).alias("st_greater_than_70_percent_mix"))
        return data


    def get_item_data(self,data:pl.DataFrame,op_map:Dict,go_to_init:bool) ->pl.DataFrame:

        """
        Get aggregated item data based on the specified operations and go-to-initial flag.

        Args:
            data (pl.DataFrame): Input DataFrame.
            op_map (Dict): Dictionary mapping column names to operation codes (0 for mean, 1 for sum).
            go_to_init (bool): Flag to determine whether to go to the initial frame/contains bunch of calculations.

        Returns:
            pl.DataFrame: Aggregated item data.

        """

        # Dictionary mapping operation codes to actual operations
        operation_map = {0:'mean',1:'sum'}

         # If go_to_init flag is True, calculate the initial frame and perform aggregation else directly perform aggregarion of selected columns
        if go_to_init:
            data = self.calculate_initial_frame(data,["ITEMID"])
            agg_dict = [eval(expr) for expr in 
                        [f"pl.col('{col}').mean()" for col in self.avg_col]+[f"pl.col('{col}').sum()" for col in self.sum_col]]
            data = data.select(agg_dict)
        else:
            agg_dict = [pl.col(col).mean() if operation_map[op_map[col]] == 0 else pl.col(col).sum() for col in list(op_map.keys())]
            data = data.select(agg_dict)
        if go_to_init:
            data = self.calculate_kpi_final(data)

        return data
    


    
    def table_change_filter(self,group:List,heirarchy:List,data_filter:Dict,DATA:pl.DataFrame,row:Dict,
                        filter_condition:Optional[pl.Series]=None) -> Union[None,pl.Series,List,pl.DataFrame]:
        
        """
    Apply primary filter based on group/sub filter columns,heirarchy, and row data.

    Args:
        group (List): List of group columns.
        heirarchy (List): List of heirarchy columns.
        data_filter (Dict): Dictionary containing filter information.
        DATA (pl.DataFrame): Input DataFrame.
        row (Dict): Dictionary containing selected row data.
        filter_condition (Optional[pl.Series]): Existing filter condition.

    Returns:
        Union[None, pl.Series, List, pl.DataFrame]: Resulting filter condition, other filter condition, existing filter condition,
        parent filter condition, columns to filter, values to filter, group columns, and the input DataFrame.

        """

        columns_to_filter = []
        values_to_filter = []
        # Extract columns and values to filter based on the selected subfilters and hierarchy
        for i in group+heirarchy:
            if i in row:
                columns_to_filter.append(i)
                values_to_filter.append(data_filter['table_changes']["row"][i])
        child = filter_condition
        other_filter_condition = None
        parent = None
        # Iterate over columns and values and create parent/child and other filter condition for table changes
        for col, val in zip(columns_to_filter, values_to_filter):
            if child is None:
                child = (DATA[col] == val)
                other_filter_condition = (DATA[col] != val)
                parent = None

            else:
                parent = child
                other_filter_condition = child & (DATA[col] != val)
                child = child & (DATA[col] == val)

        return child,other_filter_condition,filter_condition,parent,columns_to_filter,values_to_filter,group,DATA

    def call_kpi(self,DATA:pl.DataFrame,article_score:List) -> pl.DataFrame:
        DATA = DATA.with_columns(coefficient_score = pl.lit(0.0))
        length = 0
        print(article_score)
        print(gloabal_vars.SCORES)
        for col in article_score:
            print(col)
            length += 1  
            DATA = DATA.with_columns(coefficient_score = (pl.col(col)+pl.col("coefficient_score")).replace({np.inf:0,-np.inf:0}).fill_nan(0).fill_null(0))
        DATA = DATA.with_columns(coefficient_score = pl.col("coefficient_score")/length)
        print(DATA["coefficient_score"],"COES")
        return DATA

        


        





            








