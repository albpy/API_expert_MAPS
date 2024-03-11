import numpy as np
import pandas as pd
from decimal import Decimal
from sqlalchemy.orm import Session
from sqlalchemy import text
import datetime
import calendar
import traceback
from typing import List,Dict,Optional,Union,Tuple
from .schemas import Filters, Echelons
from datetime import datetime
import polars as pl
import subprocess
from rapidframes import RapiDF
from time import ctime
import os

class OTB():
    '''Store OTB specific static dfs'''
    MAX_COLS: list    = ['Channel','Family', 'SubFamily', 'Supplier','Category', 'DOM_COMM',
                       'SubCategory', 'ExtendedSubCategory', 'SubCategorySupplier', 'AssemblyCodeNickName', 
                        'Status', 'ENDOFLife','Description', 'ITEMID','budget_month','budget_week','Budget_date', 'StoreId','Store']
                        
    AVG_COLS: list    = ['final_price','FirstMargin%','act_forecast_vs_budget_percent',
                        'SalesActualsByForecast', 'LYvsACT_FCT%','initial_average_retail_price',
                        'ProposedSellThru%','budget_gross_margin_percent', 'Logistic%','adjusted_budget_gross_margin_percent','MarkdownValue','Markdown%',
                        ]

    SUM_COLS: list    = ['budget_amount','budget_cost', 'deficit','Original_BudgetAmount', 'budget_percent','relative_budget_percent', 'History_Net_Sales', 
                        'budget_vpy', 'budget_vppy', 'budget_qty' ,'ACT_FCT','Original_BudgetCostofGoods', 'PO',
                        'unit_buy_by_sku', 'COSTPRICE', 'Historical_Gross_Margin', 'sales_actual',
                        'stock_on_hand_qty','PurchasedRetailValueGrossSale','StockatRetailPrice', 
                        'net_sales_ly', 'net_sales_lly', 'gross_sales', 'gross_sales_ly',
                        'total_sku_count',
                        'SupplyCost',
                        'SALESQTY', 'DisplayItem', 'DisplayItemValue', 'COR_valueENDOfLifeStock','TYForecast', 'PurchaseRetailValueatGrossSale', 'OTBorPurchaseCost','OTBquantity',
                        #~~QTY_COl
                        'Budget_Qty_Perc', 'budget_qty_ACT_FCT_perc', 'budget_vs_py_qty_percent', 'budget_vs_ppy_qty_percent', 'QTY_Buy_By_SKU', 
                        # 'total_sku_count',
                        'total_sku_count_ty', 'total_sku_count_lly', 'total_sku_count_ly', 
                        'quantity_mix_percent',
                        'quantity_act_forecast', 'quantity_act_forecast_vs_budget_percent', 'sold_qty_ly', 'quantity_act_forecast_vs_ly_percent',
                        'quantity_act_or_forecast_per_sku', 'quantity_act_forecast_vs_ppy_percent',
                        #~~COST_COL
                        'stock_cost_ly',
                        'budget_cost_percent', 'budget_vs_act_forecast_cost_percent', 'cost_budget_per_sku', 'BudgetCostofGoods', 'budget_vs_py_cost_percent', 'budget_vs_ppy_cost_percent',
                        'cost_mix_percent', 'cost_actuals', 'cost_of_goods_ly', 'cost_act_forecast', 'cost_act_forecast_vs_budget_perc', 'cost_act_forecast_vs_ly_percent', 'cost_act_forecast_vs_ppy_percent',
                        'cost_act_or_forecast_per_sku', 
                        #~~MGN COL
                        'budget_margin_percent', 'budget_margin_mix_percent', 'budget_vs_act_forecast_margin_percent', 
                        'budget_vs_py_margin_percent', 'budget_vs_ppy_margin_percent', 
                        # 'retail_price', 
                        'ly_margin', 'margin_act_forecast_vs_ly_percent',  
                        'margin_act_forecast_vs_ppy_percent', 'margin_act_or_forecast_per_sku', 'lly_margin', 'margin_budget_per_sku', 
                        'margin_actuals', 'margin_act_forecast',
                        # units_per_sku
                        'units_per_sku_total', 'units_per_sku_ly', 'SALES_ACT_FCT_per_sku_ly', 'units_per_sku_lly', 'SALES_ACT_FCT_per_sku_lly', 
                        # units_buy_by_sku
                        'unit_buy_by_sku_total', 'unit_buy_by_sku_ly', 'unit_buy_by_sku_lly',
                        # Retail price
                        'initial_average_retail_price_ly', 'initial_average_retail_price_lly',
                        # budget/ sku
                        'budget_per_sku_qty_total',
                        # qty cols
                        'budget_qty_ly', 'budget_qty_ty', 'budget_qty_lly', 'stock_on_hand_qty_ly', 'stock_on_hand_qty_lly'
                        # budget costs
                        #'budget_cost_ty','budget_cost_ly', 'budget_cost_lly'
                        ]
    
    FLOAT_COLS: list  = ['final_price', 'initial_average_retail_price','budget_percent', 'deficit', 'unit_buy_by_sku', 'relative_budget_percent', 'act_forecast_vs_budget_percent', 'Historical_Gross_Margin', 'SalesActualsByForecast',
                        'budget_gross_margin_percent','adjusted_budget_gross_margin_percent', 'budget_vpy', 'budget_vppy', 'ProposedSellThru%',
                        'FirstMargin%', 'PurchasedRetailValueGrossSale','PurchaseRetailValueatGrossSale','TYForecast', 'Logistic%','Markdown%','LYvsACT_FCT%','PO',
                        'OTBorPurchaseCost','budget_qty','OTBquantity']
    # 'History_Net_Sales' , 
    INT_COLS: list    = ['sales_actual']
                        #,'unit_buy_by_sku', 'SKU_COUNT', 'unit_buy_by_sku_total', 'BudgetCostofGoods', 'COSTPRICE', 
                        # 'Displ#ayItem', 'COR_valueENDOfLifeStock', 'COSTPRICE']
    SCORES: dict      = {'sale':'article_score_sale', 'abc' : 'article_score_abc', 'ae':'article_score_ae', 'speed': 'article_score_speed', 'terminal' : 'article_score_terminal', 'margin' : 'article_score_margin', 
                        'sell' : 'article_score_sell', 'markdown':'article_score_markdown', 'core' :'article_score_core', 'quartile':'article_score_quartile', 'sortimeter':'article_score_sortimeter'}

    HEIRARCHY: list   = ['Channel','Family','SubFamily','Supplier','Category','DOM_COMM','SubCategory', 'ExtendedSubCategory', 'SubCategorySupplier', 'AssemblyCodeNickName', 
                        'Status', 'ENDOFLife','Description', 'ITEMID']
    SUB_FILTER: dict  = {'region':[], 'season':[], 'area':[], 'budget_year':[],'historical_year':[],'month':[],'week':[],'date':[], 'article_scores' : []}
    # 'Channel', 'Region', 'StoreId', 'Store', 'Department',  'Family', 'SubFamily', 
    #                                        'Supplier','Category', 'DOM_COMM',  'SubCategory', 'ExtendedSubCategory', 'SubCategorySupplier', 'AssemblyCodeNickName','Status', 'ENDOFLife', 
                                        #    'Description','ITEMID','historical_year', 'Budget_Year','Quarter','Month','Week','Budget_date','Day',
    PERCENT_COLS: list = ['new_budget_mix', "budget_percent","relative_budget_percent", 'act_forecast_vs_budget_percent', 'budget_gross_margin_percent', 'adjusted_budget_gross_margin_percent', 'Logistic%', 'Markdown%', 
                         'FirstMargin%', 'ProposedSellThru%', 'LYvsACT_FCT%', 'coefficient_score_mix_percent']
    TABS: dict        = {
                        'BudgetValue':     ['Check_box', 'Channel', 'Region', 'StoreId', 'Store', 'Department','Family', 'SubFamily', 
                                            'Supplier','Category', 'DOM_COMM', 'SubCategory', 'ExtendedSubCategory', 'SubCategorySupplier', 'AssemblyCodeNickName', 
                                            'Status','ENDOFLife', 'Description', 'ITEMID', 
                                            'historical_year', 'history_quarter', 'history_month', 'history_week', 'history_day', 'History_date', 
                                            'Budget_Year','budget_quarter', 'budget_month','budget_week','Budget_date', 'budget_day', 
                                            
                                            # KPI
                                            'revised_budget_amount', 'new_budget_mix',
                                            
                                            'budget_amount', 'deficit' ,'budget_percent', 'relative_budget_percent', 'budget_cost', 'act_forecast_vs_budget_percent', 
                                            'budget_qty', 'budget_vpy','budget_vppy', 'ACT_FCT', 'initial_average_retail_price', 
                                            'unit_buy_by_sku_total', 'unit_buy_by_sku', 'total_sku_count',  'units_per_sku_total', 'budget_per_sku_qty_total', 
                                            #'ly_margin_percent, 
                                            'budget_gross_margin_percent', 'adjusted_budget_gross_margin_percent', 'BudgetCostofGoods', 'Logistic%', 'SupplyCost',
                                            'FirstMargin%', 'ProposedSellThru%', 'PurchasedRetailValueGrossSale',
                                            # 'STOCK As on LY Year End @ Retail Value',
                                            'DisplayItem', 'COR_valueENDOfLifeStock', 'TYForecast',  'PurchaseRetailValueatGrossSale',  'OTBorPurchaseCost','OTBquantity',
                                            # Purchase Cost,
                                            'sales_actual',   'SALESQTY' ,  
                                            # budget qty
                                            'budget_qty_ty', 'budget_qty_ly', 'budget_qty_lly',
                                            # budget cost
                                    #        'budget_cost_ty','budget_cost_ly', 'budget_cost_lly',
                                            # 'Sales Budget',  'ACT_FCTvsBudget_perc'
                                            'net_sales_ly', 'LYvsACT_FCT%', 'initial_average_retail_price_ly', 'unit_buy_by_sku_ly', 'total_sku_count_ly',
                                            'units_per_sku_ly', 'SALES_ACT_FCT_per_sku_ly',
                                            
                                            'net_sales_lly', 'LLYvsaCT_FCT%', 'initial_average_retail_price_lly', 'unit_buy_by_sku_lly', 'total_sku_count_lly',
                                            'units_per_sku_lly', 'SALES_ACT_FCT_per_sku_lly',
                                            
            
                                            'final_price', 'MarkdownValue', 'gross_sales', 'gross_sales_ly', 'Markdown%', 'stock_on_hand_qty', 'stock_on_hand_qty_ly', 'stock_on_hand_qty_lly', 
                                            'StockatRetailPrice',  'DisplayItemValue', 
                                            
                                            'article_score_sale', 'article_score_abc', 'article_score_ae', 'article_score_speed', 'article_score_terminal', 'article_score_margin', 'article_score_sell', 'article_score_markdown', 'article_score_core', 
                                            'article_score_quartile', 'article_score_sortimeter', 'coefficient_score', 'coefficient_score_mix_percent'],

#'History_Net_Sales',   'COSTPRICE', 'Historical_Gross_Margin',
                                            
                        'BudgetCost':       ['Channel', 'Region', 'StoreId', 'Store', 'Department', 'Category', 'Family', 'SubFamily', 'Supplier',
                                            'DOM_COMM', 'SubCategory', 'ExtendedSubCategory', 'SubCategorySupplier', 'AssemblyCodeNickName',
                                            'ITEMID', 'Status', 'ENDOFLife', 'Description', 'Budget_Year', 'historical_year',
                                            'budget_cost_percent', 'budget_vs_act_forecast_cost_percent', 'cost_budget_per_sku', 'BudgetCostofGoods',
                                            'budget_vs_py_cost_percent', 'budget_vs_ppy_cost_percent', 'cost_mix_percent', 'cost_actuals', 'cost_act_forecast',
                                            'cost_act_forecast_vs_budget_perc', 'cost_of_goods_ly', 'cost_act_forecast_vs_ly_percent', 'cost_of_goods_lly', 
                                            'cost_act_forecast_vs_ppy_percent', 'cost_act_or_forecast_per_sku', 'stock_cost_ly'],

                        'BudgetQuantity':   ['Channel', 'Region', 'StoreId', 'Store', 'Department', 'Category', 'Family', 'SubFamily', 'Supplier',
                                            'DOM_COMM', 'SubCategory', 'ExtendedSubCategory', 'SubCategorySupplier', 'AssemblyCodeNickName',
                                            'ITEMID', 'Status', 'ENDOFLife', 'Description', 'Budget_Year', 'historical_year',
                                            'budget_qty', 'Budget_Qty_Perc', 'budget_qty_ACT_FCT_perc', 'budget_vs_py_qty_percent', 'budget_vs_ppy_qty_percent', 'QTY_Buy_By_SKU', 
                                            # 'Quantity_Buy_By_SKU', 
                                            'total_sku_count', 'unit_buy_by_sku', 'unit_buy_by_sku_total', 'SKU_COUNT', 'quantity_mix_percent', 'quantity_actuals',
                                            'quantity_act_forecast', 'quantity_act_forecast_vs_budget_percent', 'sold_qty_ly', 'quantity_act_forecast_vs_ly_percent',
                                            'quantity_act_or_forecast_per_sku', 'quantity_ppy', 'quantity_act_forecast_vs_ppy_percent'],
                                            
                        'BudgetMargin':     ['Channel', 'Region', 'StoreId', 'Store', 'Department', 'Category', 'Family', 'SubFamily', 'Supplier',
                                            'DOM_COMM', 'SubCategory', 'ExtendedSubCategory', 'SubCategorySupplier', 'AssemblyCodeNickName',
                                            'ITEMID', 'Status', 'ENDOFLife', 'Description', 'Budget_Year', 'historical_year',
                                            'budget_margin_percent', 'budget_margin_mix_percent',
                                            # 'retail_price', 
                                            'budget_vs_act_forecast_margin_percent', 
                                            'budget_vs_py_margin_percent', 'budget_vs_ppy_margin_percent', 'ly_margin', 'margin_act_forecast_vs_ly_percent',
                                            'total_sku_count', 'margin_budget_per_sku', 'margin_act_or_forecast_per_sku', 'lly_margin', 'margin_act_forecast_vs_ppy_percent',
                                            'margin_actuals', 'margin_act_forecast',
                                            
                                            'budget_gross_margin_percent', 'Historical_Gross_Margin', 'adjusted_budget_gross_margin_percent',
                                            'FirstMargin%', 'ProposedSellThru%']
                        }
    EDITABLE_COLS : list = ['budget_percent', 'act_forecast_vs_budget_percent', 'budget_vpy','budget_vppy', 'budget_qty', 'adjusted_budget_gross_margin_percent', 'Markdown%', 'Logistic%', 'ProposedSellThru%', 'DisplayItem', 'COR_valueENDOfLifeStock' ]
    time_columns = ['historical_year', 'Budget_Year']
    ARTICLE_SCORES : list = ['sale', 'abc', 'ae', 'speed', 'terminal', 'margin', 'sell', 'markdown', 'core', 'quartile', 'sortimeter']
    
    RANK_COLS : list = ['Check_box', 'order_index']

    filter_details = {
                    "sales_channel":"Channel",
                    "product_family":"Family",
                    "sub_families":"SubFamily",
                    "category":"Category",
                    "sub_category":"SubCategory",
                    "suppliers":"Supplier",
                    "sku":"ITEMID"
                    }

    DATA              = pl.DataFrame()
    temp          = {}
    

    def initial_frame_calculation(self,df:pl.DataFrame) ->Tuple[pl.DataFrame,pl.DataFrame]:
        '''
        Get data from DB to dataframe and preprocessing dataframe
            Args:
                df     : get a pl dataframe from rapidframe
            Returns:
                df     : data frame containing data
        '''
        print('processing...')
        
        df = df.with_columns(pl.col(list(self.SCORES.values())).replace({np.inf:0, -np.inf:0}).fill_null(0).fill_nan(0))
        # difftime = datetime.strptime(filters.forecast_date_range.fro,'%Y-%m-%d') - datetime.strptime(filters.history_date_range.fro,'%Y-%m-%d')
        # diff_forecast = datetime.strptime(filters.forecast_date_range.fro,'%Y-%m-%d') - datetime.strptime(filters.forecast_date_range.to,'%Y-%m-%d')
        #---------------------------------------------------------processing 110 operations-----------------------------------------------------
        df = df.with_columns(Check_box = pl.lit(0).cast(pl.Int8))
        df = df.with_columns(pl.col('initial_average_retail_price').replace({np.inf:0,-np.inf:0}).fill_nan(0).fill_null(0))
        df = df.with_columns(pl.col('final_price').replace({np.inf:0,-np.inf:0}).fill_nan(0).fill_null(0))
        df = df.with_columns(pl.col('budget_amount').replace({np.inf:0,-np.inf:0}).fill_nan(0).fill_null(0))
        df = df.with_columns(pl.col('net_sales_lly').replace({np.inf:0,-np.inf:0}).fill_nan(0).fill_null(0))
        df = df.with_columns(pl.col('cost_of_goods_ly').replace({np.inf:0,-np.inf:0}).fill_nan(0).fill_null(0))
        df = df.with_columns(pl.col('cost_of_goods_lly').replace({np.inf:0,-np.inf:0}).fill_nan(0).fill_null(0))
        df = df.with_columns(pl.col('stock_cost_ly').replace({np.inf:0,-np.inf:0}).fill_nan(0).fill_null(0))
        df = df.with_columns(pl.col('LINEAMOUNT').replace({np.inf:0,-np.inf:0}).fill_nan(0).fill_null(0))
        df = df.with_columns(pl.col('SALESQTY').replace({np.inf:0,-np.inf:0}).fill_nan(0).fill_null(0))
        df = df.with_columns(pl.col('sold_qty_ly').replace({np.inf:0,-np.inf:0}).fill_nan(0).fill_null(0))
        
        df = df.with_columns(pl.col('budget_qty').replace({np.inf:0, -np.inf:0}).fill_nan(0).fill_null(0))

        # df = df.with_columns(pl.col('budget_qty_ty').replace({np.inf:0, -np.inf:0}).fill_nan(0).fill_null(0))
        # df = df.with_columns(pl.col('budget_qty_ly').replace({np.inf:0, -np.inf:0}).fill_nan(0).fill_null(0))
        # df = df.with_columns(pl.col('budget_qty_lly').replace({np.inf:0, -np.inf:0}).fill_nan(0).fill_null(0))

        df = df.with_columns(pl.col('initial_average_retail_price_ty').replace({np.inf:0, -np.inf:0}).fill_nan(0).fill_null(0))
        df = df.with_columns(pl.col('initial_average_retail_price_ly').replace({np.inf:0, -np.inf:0}).fill_nan(0).fill_null(0))
        df = df.with_columns(pl.col('initial_average_retail_price_lly').replace({np.inf:0, -np.inf:0}).fill_nan(0).fill_null(0))
        df = df.with_columns(((pl.col('LINEAMOUNT')/pl.col('SALESQTY')).replace({np.inf:0,-np.inf:0}).fill_nan(0)).alias('NetPrice'))

        df = df.with_columns(((pl.col('Budget_date').cast(pl.Date))).alias('dates'))
        df = df.with_columns(Budget_Year = pl.col("dates").dt.strftime('%Y').cast(pl.Utf8))
        df = df.with_columns(((pl.col('dates').dt.weekday().cast(pl.Utf8))).alias('budget_day'))
        df = df.with_columns(((pl.col('dates').dt.quarter().cast(pl.Utf8))).alias('budget_quarter'))

        df = df.with_columns(((pl.col('dates').dt.strftime('%B'))).alias('budget_month')) # Extract Abbreviated month name (Jan, Feb, Mar).
        df = df.with_columns(((pl.col('dates').dt.strftime('%U'))).alias('budget_week')) # Extract week number (1,2,3)
        
        # df = df.with_columns(HistoricalYear = pl.col('historical_year').str.replace(',', '').replace('unknown', '').cast(pl.Float64).fill_nan(0))
        
        df = df.with_columns(Price = pl.col('final_price').cast(pl.Float64).fill_nan(0))

        df = df.with_columns(pl.lit(0).alias('DisplayItem'))

        df = df.with_columns(((pl.lit(0.05))).alias('Logistic%'))
        
        df = df.with_columns((pl.col('Logistic%').cast(float)*100).replace({np.inf : 0, -np.inf:0}).fill_nan(0).fill_null(0).alias('Logistic%'))
        
        df = df.with_columns((pl.col('DisplayItem') * pl.col('initial_average_retail_price')).alias('DisplayItemValue'))

        df = df.with_columns((pl.lit(0).alias('COR_valueENDOfLifeStock')))

        df = df.with_columns(StockatRetailPrice = (pl.col('initial_average_retail_price') * pl.col('stock_on_hand_qty')).replace({np.inf:0,-np.inf:0}).fill_nan(0).fill_null(0))
        
        

        df = df.with_columns(((pl.col('initial_average_retail_price') * pl.col('SALESQTY')) - (pl.col('NetPrice') * pl.col('SALESQTY'))).alias('Discount'))

        df = df.with_columns(((pl.col('Discount')/(pl.col('budget_amount') + pl.col('Discount'))).replace({np.inf:0,-np.inf:0}).fill_nan(0).fill_null(0)).alias('Markdown%'))

        df = df.with_columns((((pl.col('StockatRetailPrice')) - (pl.col('DisplayItemValue')) - (pl.col('COR_valueENDOfLifeStock'))).replace({np.inf:0,-np.inf:0}).fill_nan(0)).alias('TYForecast'))       
       
        df = df.with_columns((((pl.col('budget_amount').cast(pl.Float64).fill_nan(0))-((pl.col('budget_amount')*pl.col('budget_gross_margin_percent'))/100).replace({np.inf:0,-np.inf:0}).fill_nan(0).fill_null(0))).alias('BudgetCostofGoods'))

        df = df.with_columns(SupplyCost = (pl.col('BudgetCostofGoods')-(pl.col('BudgetCostofGoods')*(pl.col('Logistic%')/100))).replace({np.inf:0, -np.inf:0}).fill_nan(0).cast(float))
        
        df = df.with_columns(((((pl.col('budget_amount'))-(pl.col('SupplyCost')))/(pl.col('budget_amount'))).replace({-np.inf:0, np.inf:0}).fill_nan(0).fill_null(0)*100).alias('FirstMargin%'))        

        # df = df.with_columns(((pl.col('cost_of_goods_ly')/(pl.col('cost_of_goods_ly')+pl.col('stock_cost_ly')).replace({np.inf:0, -np.inf:0}).fill_nan(0))*100).alias('LYsellThru%'))

        df = df.with_columns((((pl.col('cost_of_goods_ly')/(pl.col('cost_of_goods_ly')+pl.col('stock_cost_ly'))).replace({np.inf:0, -np.inf:0}).fill_nan(0))*100).alias('ProposedSellThru%'))   

        df = df.with_columns(PurchasedRetailValueGrossSale = ((pl.col('budget_amount')/(100-pl.col('Markdown%')))/(pl.col('ProposedSellThru%')/100)).replace({np.inf:0, -np.inf:0}).fill_nan(0).fill_null(0))

        df = df.with_columns((((pl.col('PurchasedRetailValueGrossSale'))-pl.col('TYForecast')).replace({np.inf:0, -np.inf:0}).fill_nan(0).fill_null(0)).alias('PurchaseRetailValueatGrossSale'))

        df = df.with_columns(OTBorPurchaseCost=(pl.col('PurchaseRetailValueatGrossSale')*(100-pl.col('FirstMargin%')).replace({-np.inf:0, np.inf:0}).fill_nan(0).fill_null(0)))

        df = df.with_columns(OTBorPurchaseCost = pl.col('OTBorPurchaseCost').cast(float).fill_nan(0))

        df = df.with_columns(Original_BudgetAmount = pl.col('budget_amount'))
        
        df = df.with_columns(Sold_Quantity = pl.col('SALESQTY').cast(pl.Float64).replace({np.inf:0, -np.inf:0}).fill_nan(0).fill_null(0))

        df =  df.with_columns(StockQuantity = pl.col('stock_on_hand_qty').cast(pl.Float64).replace({np.inf:0, -np.inf:0}).fill_nan(0).fill_null(0))
        
        df = df.with_columns(SalesActualsByForecast = (pl.col('sales_actual').cast(pl.Float64)/pl.col('budget_amount')) *100)

        df = df.with_columns(((pl.col('budget_amount'))+((pl.col('initial_average_retail_price'))*(pl.col('stock_on_hand_qty')))).alias('PO'))
#-------------------------------------------------------------STOCK FOR GRAPH CLACULATION------------------------------------------------
        # df = df.with_columns(((pl.col('opening_stock_ty')+pl.col("PO"))-pl.col('closing_stock_ty')).alias('sales_ty'))
        # df = df.with_columns(((pl.col('opening_stock_ly')+pl.col("PO"))-pl.col('closing_stock_ly')).alias('sales_ly'))
        # df = df.with_columns(((pl.col('opening_stock_lly')+pl.col("PO"))-pl.col('closing_stock_lly')).alias('sales_lly'))
        
        # df = df.with_columns(((pl.col('PO')-pl.col("closing_stock_ty"))+pl.col('opening_stock_ty')).alias('sold_stock_ty'))
        # df = df.with_columns(((pl.col('PO')-pl.col("closing_stock_ly"))+pl.col('opening_stock_ly')).alias('sold_stock_ly'))
        # df = df.with_columns(((pl.col('PO')-pl.col("closing_stock_lly"))+pl.col('opening_stock_lly')).alias('sold_stock_lly'))

        # df = df.with_columns((pl.col('sales_ty')+pl.col("PO")).alias('total_stock_ty'))
        # df = df.with_columns((pl.col('sales_ly')+pl.col("PO")).alias('total_stock_ly'))
        # df = df.with_columns((pl.col('sales_lly')+pl.col("PO")).alias('total_stock_lly'))



#----------------------------------------------------------------------------------------------------------------------------------------
        # print(dict(zip(df.columns, df.dtypes)))
        
        df = df.with_columns(((pl.col('Markdown%').cast(pl.Float64))).alias('Markdown%'))
        
        df = df.with_columns(GrossSales = (pl.col('budget_amount')/(100-pl.col('Markdown%'))).replace({np.inf:0, -np.inf:0}).fill_nan(0).fill_null(0) * 100)

        df = df.with_columns(MarkdownValue = (pl.col('gross_sales')-pl.col('net_sales_ly')).replace({np.inf:0, -np.inf:0}).fill_nan(0).fill_null(0))

        df = df.with_columns(((pl.col('ProposedSellThru%').cast(pl.Float64))).alias('ProposedSellThru%'))
        # df = df.with_columns(with_opening_stock = pl.col('opening_stock').apply(lambda x: isinstance(x, int) and x != 0, return_dtype=pl.Boolean))
        # df = df.with_columns(with_opening_stock = pl.col('with_opening_stock').cast(pl.Float64))
        # df = df.with_columns(with_transactions = pl.col("opening_stock") - pl.col("closing_stock") > 0)
        # df = df.with_columns(with_transactions = pl.col('with_transactions').cast(pl.Float64))
        # if df['with_opening_stock'].sum() != 0:
        #     df = df.with_columns(History_Net_Sales = (pl.col('net_sales_lly') + pl.col('net_sales_ly') + pl.col('sales_actual')).cast(pl.Float64)/(pl.col('with_opening_stock').sum()))
        # else:
        #     df = df.with_columns(History_Net_Sales = (pl.col('net_sales_lly').cast(pl.Float64) + pl.col('net_sales_ly').cast(pl.Float64) + pl.col('sales_actual').cast(pl.Float64)).cast(pl.Float64)/(pl.col('with_transactions').sum()))
        
        df = df.with_columns(History_Net_Sales = (pl.col('net_sales_lly') + pl.col('net_sales_ly') + pl.col('sales_actual')).cast(pl.Float64).replace({np.inf:0, -np.inf:0}).fill_nan(0).fill_null(0))

        # depending on the year selected...
        
        df = df.with_columns(Purchase_RetailValueGrossSale = (pl.col('PurchasedRetailValueGrossSale').cast(pl.Float64)-pl.col('TYForecast')).replace({np.inf:0, -np.inf:0}).fill_nan(0).fill_null(0).cast(float))
        
        df = df.with_columns(((pl.col('History_Net_Sales').cast(pl.Float64))).alias('History_Net_Sales'))
        
        df = df.with_columns(((pl.col('COSTPRICE').cast(pl.Float64))).alias('COSTPRICE'))
     
        #           *           ^               Quantity            *                   $               %
        df = df.with_columns(((pl.col('budget_amount'))-((pl.col('final_price')) * (pl.col('budget_qty')))).alias('deficit'))

        df = df.with_columns((pl.col('History_Net_Sales')-pl.col('COSTPRICE')).alias('Historical_Gross_Margin'))

        df = df.with_columns(((pl.col('budget_qty')/pl.col('budget_qty').sum())*100).alias('Budget_Qty_Perc'))
        
        df = df.with_columns(((pl.col('budget_qty')/pl.col('quantity_actuals')).replace({np.nan:0, -np.nan:0}).fill_nan(0)*100).alias('budget_qty_ACT_FCT_perc'))      
        
        df = df.with_columns(((pl.col('budget_qty')/pl.col('sold_qty_ly')).replace({np.inf:0, -np.inf:0}).fill_nan(0)).alias('budget_vs_py_qty_percent'))

        df = df.with_columns(((pl.col('budget_qty')/pl.col('quantity_ppy')).replace({-np.inf:0, np.inf:0}).fill_nan(0)*100).alias('budget_vs_ppy_qty_percent'))
        
        # creating a struct to get unique sku number
        subset = ['ITEMID']
        df= df.with_columns((pl.struct(subset).is_first_distinct().alias('total_sku_count')))
        
        subset = ['ITEMID_ty']
        df = df.with_columns((pl.struct(subset).is_first_distinct().alias('total_sku_count_ty')))  
        
        subset = ['ITEMID_ly']
        df = df.with_columns((pl.struct(subset).is_first_distinct().alias('total_sku_count_ly')))
        
        subset = ['ITEMID_lly']
        df = df.with_columns((pl.struct(subset).is_first_distinct().alias('total_sku_count_lly')))

        df = df.with_columns(((pl.col('budget_qty')/pl.col('total_sku_count')-1).replace({np.inf:0, -np.inf:0}).fill_nan(0)).alias('QTY_Buy_By_SKU'))

        try:
            df = df.with_columns(((pl.col('budget_qty')/pl.col('total_sku_count')).replace({np.inf:0,-np.inf:0}).fill_nan(0)).alias('unit_buy_by_sku'))
        except Exception as e:
            print(f"error units_per_sku is : {e}")
        
        df = df.with_columns(((pl.col('budget_qty_ly')/pl.col('total_sku_count_ly')).replace({np.inf:0, -np.inf:0}).fill_nan(0)).alias('unit_buy_by_sku_ly'))

        df = df.with_columns(((pl.col('budget_qty_lly')/pl.col('total_sku_count_lly')).replace({np.inf:0, -np.inf:0}).fill_nan(0)).alias('unit_buy_by_sku_lly'))

        df = df.with_columns((((pl.col('LINEAMOUNT'))/pl.col('total_sku_count')).replace({np.inf:0,-np.inf:0}).fill_nan(0)).alias('units_per_sku_total'))

        df = df.with_columns(((pl.col('net_sales_ly')/pl.col('total_sku_count_ly')).replace({np.inf:0,-np.inf:0}).fill_nan(0)).alias('units_per_sku_ly'))

        df = df.with_columns(((pl.col('net_sales_lly')/pl.col('total_sku_count_lly')).replace({np.inf:0,-np.inf:0}).fill_nan(0)).alias('units_per_sku_lly'))

        df = df.with_columns(((pl.col('budget_qty')/pl.col('total_sku_count')).replace({np.inf:0, -np.inf:0}).fill_nan(0).fill_null(0)).alias('unit_buy_by_sku_total'))
        
        df = df.with_columns((((pl.col('sold_qty_ly')*100)/(pl.col('sold_qty_ly').sum())).replace({np.inf:0, -np.inf:0}).fill_nan(0)).alias('quantity_mix_percent'))
        
        df = df.with_columns((pl.col('budget_qty')+pl.col('quantity_actuals')).alias('quantity_act_forecast'))
        
        df = df.with_columns(((pl.col('budget_qty')/pl.col('quantity_act_forecast')).replace({np.inf:0,-np.inf:0}).fill_nan(0)*100).alias('quantity_act_forecast_vs_budget_percent'))
        
        df = df.with_columns(((pl.col('quantity_act_forecast')/pl.col('sold_qty_ly')).replace({np.inf:0,-np.inf:0}).fill_nan(0)*100).alias('quantity_act_forecast_vs_ly_percent'))
        
        df = df.with_columns(((pl.col('quantity_act_forecast')/pl.col('total_sku_count')).replace({np.inf:0,-np.inf:0}).fill_nan(0)*100).alias('quantity_act_or_forecast_per_sku'))
        
        df = df.with_columns(((pl.col('quantity_act_forecast')/pl.col('quantity_ppy')).replace({np.inf:0,-np.inf:0}).fill_nan(0)*100).alias('quantity_act_forecast_vs_ppy_percent'))
        
        df = df.with_columns(((pl.col('budget_amount') /pl.col('total_sku_count')).replace({-np.inf:0, np.inf:0}).fill_nan(0)*100).alias('budget_per_sku_qty_total'))
        
        ##~~Consummate QTY
        #                   *                   \               cost            *                   $                       #
        df = df.with_columns(Original_BudgetCostofGoods = pl.col('BudgetCostofGoods'))

        df = df.with_columns(((pl.col('budget_cost')*100)/(pl.col('budget_cost').sum())).alias('budget_cost_percent'))

        df = df.with_columns(((pl.col('budget_cost')+pl.col('cost_actuals')).replace({-np.inf:0, np.inf:0}).fill_nan(0)).alias('cost_act_forecast'))

        df = df.with_columns(((pl.col('budget_cost')/pl.col('cost_act_forecast'))*100).alias('budget_vs_act_forecast_cost_percent'))

        df = df.with_columns(((pl.col('budget_cost')/pl.col('total_sku_count')).replace({-np.inf:0, np.inf:0}).fill_nan(0)).alias('cost_budget_per_sku'))

        df = df.with_columns(((pl.col('budget_cost')/pl.col('cost_of_goods_ly')).replace({-np.nan:0, np.nan:0}).fill_nan(0)*100).alias('budget_vs_py_cost_percent'))

        df = df.with_columns(((pl.col('budget_cost')/pl.col('cost_of_goods_lly')).replace({-np.inf:0, np.inf:0}).fill_nan(0)*100).alias('budget_vs_ppy_cost_percent'))

        df = df.with_columns(((pl.col('cost_of_goods_ly')*100)/(pl.col('cost_of_goods_ly').sum()).replace({-np.inf:0, np.inf:0}).fill_nan(0)).alias('cost_mix_percent'))

        df = df.with_columns(((pl.col('cost_act_forecast')/pl.col('budget_cost'))*100).alias('cost_act_forecast_vs_budget_perc'))

        df = df.with_columns(((pl.col('cost_act_forecast')/pl.col('cost_of_goods_ly'))*100).alias('cost_act_forecast_vs_ly_percent'))

        df = df.with_columns((((pl.col('cost_act_forecast').replace({-np.inf:0, np.inf:0}).fill_nan(0)/pl.col('cost_of_goods_lly').replace({-np.inf:0, np.inf:0}).fill_nan(0)).replace({-np.inf:0, np.inf:0}).fill_nan(0))*100).alias('cost_act_forecast_vs_ppy_percent'))

        df = df.with_columns(((pl.col('cost_act_forecast') /pl.col('total_sku_count')).replace({-np.inf:0, np.inf:0}).fill_nan(0)*100).alias('cost_act_or_forecast_per_sku'))

        
        #Buy_cost_by_sku , cost_per_sku
        #~~~Consummate cost~~~~~~#
        #        *          #             \               #####Margin#####       *         #              $             #
        
        df = df.with_columns(sales_actual = pl.col('sales_actual').cast(pl.Int64))

        df = df.with_columns((((pl.col('sales_actual') - pl.col('cost_actuals'))/pl.col('sales_actual')).replace({np.inf:0,-np.inf:0}).fill_nan(0)).alias('margin_actuals'))

        df = df.with_columns((pl.col('budget_gross_margin_percent')/100).alias('budget_gross_margin'))

        df = df.with_columns(((pl.col('budget_gross_margin')/pl.col('budget_amount'))*100).alias('budget_margin_percent'))

        df = df.with_columns(((pl.col('budget_gross_margin')+pl.col('margin_actuals')).replace({-np.inf:0, np.inf:0}).fill_nan(0)).alias('margin_act_forecast'))

        df = df.with_columns(((pl.col('budget_gross_margin')*100)/(pl.col('budget_gross_margin').sum())).alias('budget_margin_mix_percent'))

        df = df.with_columns((((pl.col('budget_gross_margin')/pl.col('margin_act_forecast'))*100)).alias('budget_vs_act_forecast_margin_percent'))

        df = df.with_columns(((((pl.col('net_sales_ly') - pl.col('cost_of_goods_ly'))/pl.col('net_sales_ly')).fill_nan(0))*100).alias('ly_margin_percent'))

        df = df.with_columns(((pl.col('net_sales_ly').fill_nan(0))/100).alias('ly_margin'))

        df = df.with_columns(((pl.col('margin_act_forecast') /(pl.col('ly_margin_percent')*100))*100).alias('margin_act_forecast_vs_ly_percent'))

        df = df.with_columns(((pl.col('margin_act_forecast') /pl.col('total_sku_count'))*100).alias('margin_act_or_forecast_per_sku'))

        df = df.with_columns(((((pl.col('net_sales_lly') - pl.col('cost_of_goods_lly'))/pl.col('net_sales_lly')).fill_nan(0))*100).alias('lly_margin_percent'))

        df = df.with_columns(((pl.col('lly_margin_percent').replace({np.inf:0,-np.inf:0}).fill_nan(0))/100).alias('lly_margin'))

        df = df.with_columns(((pl.col('margin_act_forecast') /(pl.col('lly_margin_percent')*100)).replace({-np.inf:0, np.inf:0}).fill_nan(0).fill_null(0) *100).alias('margin_act_forecast_vs_ppy_percent'))

        df = df.with_columns(((pl.col('budget_gross_margin')/pl.col('total_sku_count')).replace({np.inf:0, -np.inf:0}).fill_nan(0).fill_null(0)).alias('margin_budget_per_sku'))

        df = df.with_columns(((pl.col('budget_gross_margin')/(pl.col('ly_margin_percent')*100)).replace({-np.inf:0, np.inf:0}).fill_nan(0).fill_null(0)*100).alias('budget_vs_py_margin_percent'))

        df = df.with_columns(((pl.col('budget_gross_margin')/(pl.col('lly_margin_percent')*100)).replace({-np.inf:0, np.inf:0}).fill_nan(0).fill_null(0)*100).alias('budget_vs_ppy_margin_percent'))

        df = df.with_columns((((pl.col('PurchaseRetailValueatGrossSale'))/(pl.col('initial_average_retail_price'))).replace({np.inf:0, -np.inf:0}).fill_nan(0).fill_null(0)).alias('OTBquantity'))
        
        df = df.with_columns((pl.lit(0.0)).alias("coefficient_score"))
        
        df = df.with_columns(coefficient_score_mix_percent = (pl.col('coefficient_score')/pl.col('coefficient_score').sum()).replace({-np.inf:0, np.inf:0}).fill_nan(0).fill_null(0)*100)

        group = []

        self.SUB_FILTER = self.call_filter(df,self.SUB_FILTER,group,df)
        
        wr=False
        self.DATA = self.calculate_df(df,wr)
        
        # self.DATA[self.MAX_COLS]        = self.DATA[self.MAX_COLS].astype(str)
        # self.DATA[self.MAX_COLS] = self.DATA[self.MAX_COLS].cast(pl.Utf8)
        print(self.DATA.select(pl.all().null_count()).to_dicts()[0],'lst DATA null')

        return self.DATA

 
    #feel free to change this code , i did this in hurry
    def call_filter(self,data:pl.DataFrame,SUB_FILTER:dict,group:list,DATA:pl.DataFrame):
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
                     
            keys = ['store','region','Channel','budget_year','Quarter','month','week','Day','date','history_year','history_Quarter',
            'history_month','history_week','history_Day','history_dates',
            'family', 'sub_family', 'supplier', 'category', 'dom_comm', 'sub_category', 'extended_sub_category', 'sub_category_supplier']

            values = ['Store','Region','Channel','Budget_Year','budget_quarter','budget_month','budget_week','budget_day','Budget_date','historical_year','history_quarter'
            ,'history_month','history_week','history_day',"History_date",
            'Family', 'SubFamily', 'Supplier', 'Category', 'DOM_COMM', 'SubCategory', 'ExtendedSubCategory', 'SubCategorySupplier']

            print(group, 'in call filter')
            for i,v in zip(keys,values):
                print('v is',v, end =' ')
                if v not in group:
                    print('not in group')
                    if v in ['Budget_Year','historical_year']:
                        # SUB_FILTER[i]        = list(data[v].astype(float).unique())
                        SUB_FILTER[i]        = list(data[v].replace("unknown", 0.0).cast(pl.Float32).unique())
                        # SUB_FILTER[i]        = sorted(list(data[v].replace('unknown',0.0).cast(pl.Float32).unique()),key=lambda x:(x is None,x))

                    elif v == 'dates':
                        # SUB_FILTER['date']   = list(data[v].astype(str).unique())
                        SUB_FILTER['date']   = list(data[v].cast(str).unique())
                    elif v == 'history_dates':
                        # SUB_FILTER['i']   = list(data[v].astype(str).unique())
                        SUB_FILTER['i']   = list(data[v].cast(str).unique())
                    else:
                        SUB_FILTER[i]        = list(data[v].unique())
            
                SUB_FILTER['article_score'] = ['sale', 'abc', 'ae', 'speed', 'terminal', 'margin', 'sell',
        'markdown', 'core', 'quartile', 'sortimeter']

                # else:
                #     print('here came')
                #     if v in ['Budget_Year','historical_year']:
                #         SUB_FILTER[i]        = list(DATA[v].astype(float).unique())
                #     elif v == 'Date':
                #         SUB_FILTER['date']   = list(DATA['Date'].astype(str).unique())
                #     else:
                #         SUB_FILTER[i]        = list(DATA[v].unique())
            #                           
            print('SUB_FILTER contents:', SUB_FILTER)

        except Exception as e:
            print(traceback.format_exc())
            print(f'error is {e}')
   
        return SUB_FILTER
    
    def calculate_df(self, data: pl.DataFrame,wr:bool=False):
        '''
        Calculation on the Dataframe
            Args:
                df : DataFrame to be calculated
            Returns:
                data: Calculated data frame
        '''
        print('calculating the df...')
        print(data)
        
# XII Calculations With one condition
        data = data.with_columns((((pl.col('budget_amount')/pl.col('net_sales_ly')).replace({np.inf:0,-np.inf:0}).fill_nan(0))*100).alias('budget_vpy'))
        
        data = data.with_columns((((pl.col('budget_amount')/pl.col('net_sales_lly')).replace({np.inf:0,-np.inf:0}).fill_nan(0))*100).alias('budget_vppy'))
        
        # data = data.with_columns(Original_BudgetAmount = pl.col('budget_amount'))

        total_budget = data.select(pl.col('budget_amount').sum())
        
        data = data.with_columns((pl.when(pl.col('budget_amount')/total_budget!=0).then((pl.col('budget_amount')/total_budget)*100).otherwise(0)).alias('relative_budget_percent'))
       

        data = data.with_columns(((pl.col('budget_amount')/pl.col('budget_amount').sum())*100).alias('budget_percent'))
        if wr:
            data = data.with_columns(((pl.col('budget_amount').sum()) *0.01* (pl.col('budget_percent'))).alias('budget_amount'))
        
        data = data.with_columns(((pl.col('sales_actual').cast(pl.Int8))))
        
        data = data.with_columns(((pl.col('budget_amount').cast(pl.Float64)+pl.col('sales_actual').cast(pl.Float64))).alias('ACT_FCT'))
        
        data = data.with_columns(((((pl.col('budget_amount'))/(pl.col('ACT_FCT'))).replace({np.inf:0,-np.inf:0}).fill_nan(0).fill_null(0)) *100).alias('act_forecast_vs_budget_percent'))
        
        data = data.with_columns((((pl.col('net_sales_ly')/(pl.col('net_sales_ly').sum())).replace({np.inf:0,-np.inf:0}).fill_nan(0).fill_null(0))*100).alias('LYvsACT_FCT%'))
        
        data = data.with_columns((((pl.col('LYvsACT_FCT%')/(pl.col('total_sku_count_ly'))).replace({np.inf:0,-np.inf:0}).fill_nan(0).fill_null(0))).alias('SALES_ACT_FCT_per_sku_ly'))

        data = data.with_columns((((pl.col('net_sales_lly')/(pl.col('net_sales_lly').sum())).replace({np.inf:0,-np.inf:0}).fill_nan(0).fill_null(0))*100).alias('LLYvsaCT_FCT%'))
        
        data = data.with_columns((((pl.col('LLYvsaCT_FCT%')/(pl.col('total_sku_count_lly'))).replace({np.inf:0,-np.inf:0}).fill_nan(0).fill_null(0))*100).alias('SALES_ACT_FCT_per_sku_lly'))

        data = data.with_columns((pl.col('initial_average_retail_price')).alias('initial_average_retail_price'))
#********************************************Polaris*************************************************************************************************************************************
        return data
    
    def aggregation(self):
        agg_dict = ({col : 'mean' for col in self.AVG_COLS})
        agg_dict.update({col: 'sum' for col in self.SUM_COLS})
        return agg_dict

    def secondary_filter(self,data: pl.DataFrame,filter_data: Filters,sub_filter_state:bool,
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
        print("In the Secondary filter")
        key_map = {'Store_Name':'Store','region':'Region','Channel':'Channel','BudgetYear':'Budget_Year','Quarter':'budget_quarter',
        'month':'budget_month','week':'budget_week','Day':'budget_day','BudgetDate':'Budget_date','HistoricalYear':'historical_year','history_Quarter':'history_quarter',
        'history_month':'history_month','history_week':'history_week','history_Day':'history_day','history_dates':"History_date", 'article_score' : OTB().SCORES
        }
    
        try:
            print(filter_data.secondary_filter.dict(),"DICTIONARY")
            # Iterate over filter_data keys and values
            for key,values in filter_data.secondary_filter.dict().items():
                # Skip if values are empty
                if not values or key == 'article_score':
                    continue

                
                sub_filter_state = True
                # obtaind filter condition based on the key and values
                if key_map[key] in ['Budget_Year']:
                    
                    values = [str(i) for i in values]
                    new_condition = data[key_map[key]].is_in(values)
                else:
                    # values = [str(i) for i in values]
                    print(key_map[key])
                    print(values)
                    new_condition = data[key_map[key]].is_in(values)
                group.append(key_map[key])

                if filter_condition  is None:
                    filter_condition = new_condition
                    print(filter_condition,"FCC")
                else:
                    filter_condition = filter_condition & new_condition
                    print(filter_condition,"FCC11")

        except Exception as e:
            print(traceback.format_exc())
            print(f"Error applyting filters:{e}")
        
        # If sub_filter_state is not True, reset group, filter_condition, and sub_filter_state
        if not sub_filter_state == True:
            group = []
            filter_condition = None
            sub_filter_state = False    
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


    
    def main_filters(self, data : pl.DataFrame, filter_data : Dict, sub_filter_state : bool, group:list, filter_condition:[Optional] =None):
        key_map = {'sales_channel':'Channel','product_family': 'Family', 'sub_families': 'SubFamily', 'suppliers' : 'Supplier', 'category':'Category',
        'sub_category' : 'SubCategory', 'sku' : 'ITEMID'}
        import traceback
        import sys
        filter_data = filter_data
        print(filter_data, 'FDF')
        print(data['Channel'].unique(), 'unique')
        # print('tsdatd')
        values = []
        filter_condition = None
        new_condition = None
        try:
            if filter_data['group_by']['columns']:
                new_data = pl.DataFrame()
                for cols in filter_data['group_by']['columns']:
                    if cols in filter_data:
                        if filter_data[cols] != []:
                            # print(cols)
                            # print(filter_data[cols], 'sssss')
                            if cols == 'sales_channel':
                                print('Channel_in')
                            # for one in filter_data[cols]:
                            #     print(one, 'primitive')
                                # if one in list(data['Channel'].unique()):
                                #     print(one, 'oneee')
                                
                                values = [str(i) for i in filter_data[cols] if i in data['Channel'].unique()]
                                # print(values, 'valls_cha')
                                new_condition = data[key_map[cols]].is_in(values)
                                # print(new_condition)
                                print(len(new_condition),'len of new cond')
                                if filter_condition is None:
                                    print('fir')
                                    filter_condition = new_condition
                                else:
                                    print('sec')
                                    filter_condition = filter_condition & new_condition
                                print(filter_condition.value_counts(), 'fil_cha')
                                new_data = new_data.with_columns(data.filter(filter_condition))
                                # print(new_data)
                            if cols == 'product_family':
                                filter_condition = None
                                print(type(filter_condition))
                                # print(filter_condition.value_counts(), 'fil_fam')
                                print('Family_in')
                                if new_data.is_empty():
                                    values = [str(i) for i in filter_data[cols] if i in data['Family'].unique()]
                                    new_condition = data[key_map[cols]].is_in(values)
                                else:
                                    values = [str(i) for i in filter_data[cols] if i in new_data['Family'].unique()]
                                    new_condition = new_data[key_map[cols]].is_in(values)
                                print(values, 'valls_fam')
                                
                                if filter_condition is None:
                                    print('1')
                                    filter_condition = new_condition
                                else:
                                    print('2')
                                    # print(filter_condition, 'fil_cond')
                                    # print(new_condition, 'new cond')
                                    filter_condition = filter_condition & new_condition
                                    print(filter_condition.value_counts(), 'fil_fam_hve_fil')
                                    
                                if new_data.is_empty():
                                    new_data = new_data.with_columns(data.filter(filter_condition))
                                else:
                                    new_data = new_data.filter(filter_condition)
                            if cols == 'sub_families':
                                filter_condition = None
                                print('SubFamily_in')
                                if new_data.is_empty():
                                    values = [str(i) for i in filter_data[cols] if i in data['Family'].unique()]
                                    new_condition = data[key_map[cols]].is_in(values)
                                else:
                                    values = [str(i) for i in filter_data[cols] if i in new_data['Family'].unique()]
                                    new_condition = new_data[key_map[cols]].is_in(values)
                                print(values, 'valls_fam')
                                
                                if filter_condition is None:
                                    print('1')
                                    filter_condition = new_condition
                                else:
                                    print('2')
                                    # print(filter_condition, 'fil_cond')
                                    # print(new_condition, 'new cond')
                                    filter_condition = filter_condition & new_condition
                                    print(filter_condition.value_counts(), 'fil_fam_hve_fil')
                                    
                                if new_data.is_empty():
                                    new_data = new_data.with_columns(data.filter(filter_condition))
                                else:
                                    new_data = new_data.filter(filter_condition)

                            
                            if cols == 'suppliers':
                                filter_condition = None
                                print('Supplier_In')
                                if new_data.is_empty():
                                    values = [str(i) for i in filter_data[cols] if i in data['Family'].unique()]
                                    new_condition = data[key_map[cols]].is_in(values)
                                else:
                                    values = [str(i) for i in filter_data[cols] if i in new_data['Family'].unique()]
                                    new_condition = new_data[key_map[cols]].is_in(values)
                                print(values, 'valls_fam')
                                
                                if filter_condition is None:
                                    print('1')
                                    filter_condition = new_condition
                                else:
                                    print('2')
                                    # print(filter_condition, 'fil_cond')
                                    # print(new_condition, 'new cond')
                                    filter_condition = filter_condition & new_condition
                                    # print(filter_condition.value_counts(), 'fil_fam_hve_fil')
                                    
                                if new_data.is_empty():
                                    new_data = new_data.with_columns(data.filter(filter_condition))
                                else:
                                    new_data = new_data.filter(filter_condition)
                            if cols == 'category':
                                filter_condition = None
                                print('Category_In')
                                if new_data.is_empty():
                                    values = [str(i) for i in filter_data[cols] if i in data['Family'].unique()]
                                    new_condition = data[key_map[cols]].is_in(values)
                                else:
                                    values = [str(i) for i in filter_data[cols] if i in new_data['Family'].unique()]
                                    new_condition = new_data[key_map[cols]].is_in(values)
                                print(values, 'valls_fam')
                                
                                if filter_condition is None:
                                    print('1')
                                    filter_condition = new_condition
                                else:
                                    print('2')
                                    # print(filter_condition, 'fil_cond')
                                    # print(new_condition, 'new cond')
                                    filter_condition = filter_condition & new_condition
                                    print(filter_condition.value_counts(), 'fil_fam_hve_fil')
                                    
                                if new_data.is_empty():
                                    new_data = new_data.with_columns(data.filter(filter_condition))
                                else:
                                    new_data = new_data.filter(filter_condition)

                            if cols == 'sub_category':
                                filter_condition = None
                                print('SubCategory_In')
                                if new_data.is_empty():
                                    values = [str(i) for i in filter_data[cols] if i in data['Family'].unique()]
                                    new_condition = data[key_map[cols]].is_in(values)
                                else:
                                    values = [str(i) for i in filter_data[cols] if i in new_data['Family'].unique()]
                                    new_condition = new_data[key_map[cols]].is_in(values)
                                print(values, 'valls_fam')
                                
                                if filter_condition is None:
                                    print('1')
                                    filter_condition = new_condition
                                else:
                                    print('2')
                                    # print(filter_condition, 'fil_cond')
                                    # print(new_condition, 'new cond')
                                    filter_condition = filter_condition & new_condition
                                    print(filter_condition.value_counts(), 'fil_fam_hve_fil')
                                    
                                if new_data.is_empty():
                                    new_data = new_data.with_columns(data.filter(filter_condition))
                                else:
                                    new_data = new_data.filter(filter_condition)
                            if cols == 'sku':
                                filter_condition = None
                                print('ITEMID_In')
                                if new_data.is_empty():
                                    values = [str(i) for i in filter_data[cols] if i in data['Family'].unique()]
                                    new_condition = data[key_map[cols]].is_in(values)
                                else:
                                    values = [str(i) for i in filter_data[cols] if i in new_data['Family'].unique()]
                                    new_condition = new_data[key_map[cols]].is_in(values)
                                print(values, 'valls_fam')
                                
                                if filter_condition is None:
                                    print('1')
                                    filter_condition = new_condition
                                else:
                                    print('2')
                                    # print(filter_condition, 'fil_cond')
                                    # print(new_condition, 'new cond')
                                    filter_condition = filter_condition & new_condition
                                    print(filter_condition.value_counts(), 'fil_fam_hve_fil')
                                    
                                if new_data.is_empty():
                                    new_data = new_data.with_columns(data.filter(filter_condition))
                                else:
                                    new_data = new_data.filter(filter_condition)       
        except Exception:
            print('hhjhkjhdkjasj')
            print(traceback.format_exc())
            print(sys.exc_info()[2])
        return new_data

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
        print(group, 'grrrp')
        for i in group+heirarchy:
            if i in row:
                columns_to_filter.append(i)
                values_to_filter.append(row[i]) # data_filter['table_changes']["row"]
        child = filter_condition
        other_filter_condition = None
        parent = None
        print('chhh', child)
        print(filter_condition, 'filterconti')
        
        # Iterate over columns and values and create parent/child and other filter condition for table changes
        print('columns_to_filter::',  columns_to_filter)
        print('values_to_filter::', values_to_filter)
        for col, val in zip(columns_to_filter, values_to_filter):
            if child is None:
                print("in if")
                print('ch_None')
                child = (DATA[col] == val)
                other_filter_condition = (DATA[col] != val)
                parent = None

            else:
                print("in else")
                print('Hv_chd')
                parent = child
                other_filter_condition = child & (DATA[col] != val)
                child = child & (DATA[col] == val)
        print(child, 'and' ,parent)
        return child,other_filter_condition,filter_condition,parent,columns_to_filter,values_to_filter,group,DATA

    def change_percent(self, grouped_df,other_grouped_df,increase,colID):

        summation = grouped_df[colID].fill_nan(0).sum()
        #   grouped_df[colID] = colID + (colID*increase)/summation
        grouped_df = grouped_df.with_columns((grouped_df[colID]+(grouped_df[colID]*increase)/summation).alias(colID))
        #   unsummation = other_grouped_df[colID].sum()
        unsummation = other_grouped_df[colID].fill_nan(0).sum()
        
        other_grouped_df = other_grouped_df.with_columns((other_grouped_df[colID] - (other_grouped_df[colID]*increase)/unsummation).alias(colID))
        
        frames =  [grouped_df,other_grouped_df]
        
        df = pl.concat(frames)
        
        return df

    def change_value(self, grouped_df,other_grouped_df,  increase,colID):
        summation = grouped_df[colID].sum()
        # grouped_df[colID] = grouped_df[colID] + (grouped_df[colID]*increase)/summation
        print(grouped_df.columns, 'grouped df cols')
        
        grouped_df = grouped_df.with_columns(pl.col('colID')+(pl.col('colID')*increase)/summation)
        
        return pl.concat([grouped_df,other_grouped_df])
                    

    def distribute_value(self, child_df, siblings_df, increase, columnID):
        child_sum =child_df[columnID].sum()
        child_df[columnID] =  (child_sum+increase)*(child_df[columnID]/child_sum)
        siblings_sum =siblings_df[columnID].sum()
        siblings_df[columnID] = (siblings_sum-increase)*(siblings_df[columnID]/siblings_sum)
        return child_df,siblings_df
    
    def drill_down_percentage(self,df,columnID,newValue,ref_col):
        df[columnID] = (df[ref_col]/df[ref_col].mean()) * newValue
        return df
    
    def drill_down_cor(self,df,columnID,newValue):

        print(df.select(pl.col('StockatRetailPrice')).sum())

        df = df.with_columns(StockatRetailPrice = pl.col('StockatRetailPrice').replace({np.inf:0,-np.inf:0}).fill_null(0).fill_nan(0))

        df = df.with_columns((((pl.col('StockatRetailPrice')/pl.col('StockatRetailPrice').sum())).replace({np.inf:0,-np.inf:0}).fill_nan(0).cast(pl.Float64) * newValue).alias(columnID))

        return df[columnID]

    def drill_down_display(self,df,columnID,newValue):

        df = df.with_columns(columnID = (pl.col('initial_average_retail_price')/pl.col('initial_average_retail_price').sum())*newValue)
        
        return df[columnID] 

    def get_max_session_id(email_address,module_id):
        email_address = email_address.replace('"',"'")
        module_id = module_id.replace('"',"'")

        query = f"""  SELECT "session_id","table_name" FROM users_tables
        WHERE "session_id" IN (SELECT COALESCE(MAX("session_id"),0) FROM users_tables WHERE "email_address" 
        = '{email_address}' AND module_id = '{module_id}') """
        return query

    def get_session_id(self,db: Session,email_address:str,module_id:str):

        result = db.execute(text(get_max_session_id(email_address,module_id)))
        rows = [list(row) for row in result]
        df = pd.DataFrame(columns=['session_id','table_name'], data=rows)
        values = df['session_id'].values
        table_maps = {'one':'two','two':'three','three':'one','zero':'one'}

        if values.size == 0:
            session_id = 1
            table_last_label = 'zero'
        else:
            table_name = df['table_name'].values[0]
            session_id = int(values[0]) + 1
            table_last_label = table_name.split('_')[-1]
        table_label = table_maps[table_last_label]
        return session_id,table_label


    def update_users_tables(self,db:Session,email_address,module_id,session_id,table_name):
        new_record = Users_tables(email_address=email_address,module_id=module_id,session_id=session_id,table_name=table_name)
        db.add(new_record)
        db.commit()
        db.refresh(new_record)

    def save_table_to_db(self,db:Session,df:pl.DataFrame,table_name:str):
        
        print(df.columns)
        max_icols = ["Description","Department","Category","Family","SubFamily","SubCategory",
                    "ExtendedSubCategory","SubCategorySupplier","AssemblyCodeNickName","ENDOFLife","DOM_COMM","Status",
                    "Store","area",'Region',"history_quarter","history_week","history_month",
                    "Budget_Year","budget_day","Supplier", 'budget_quarter', 'budget_week', 'budget_month']
        avg_icols = []
        sumi_cols = ['budget_amount',"budget_cost",'budget_qty', 'PurchaseRetailValueatGrossSale', 'OTBorPurchaseCost', 'OTBquantity', 
                     'stock_on_hand_qty', 'stock_on_hand_qty_ly', 'stock_on_hand_qty_lly'] #bud_am,cos ,qty
        subset = ["ITEMID","Channel","INVENTLOCATIONID","Budget_date"]      # otborcost, purcseat, otb qty  
        
        ap_columns = {'Channel' : 'channel',
                        'budget_amount':'budget_amount',
                        'PurchaseRetailValueatGrossSale' : 'otb_amount',
                        'OTBorPurchaseCost' : 'otb_cost',
                        'OTBquantity' : 'otb_qty',
                        # 'Budget_date':'BDate', #netsale,lly, ty, sales_cost_ly, SALESQTY
                        'Budget_Year':'budget_year',
                        'Category':'Category','budget_day':'budget_day','ITEMID':'ITEMID',
                        'budget_cost':'budget_cost','budget_qty':'budget_qty','budget_quarter':'budget_quarter',
                        'Store':'Store','budget_month':'budget_month','Region':'Region',
                        'SubFamily':'SubFamily','budget_week':'budget_week',
                        'stock_on_hand_qty' : 'stock_qty_actual',
                        'stock_on_hand_qty_ly' : 'stock_qty_ly',
                        'stock_on_hand_qty_lly' : 'stock_qty_lly'
                        }
        
        # columns = ["Channel","INVENTLOCATIONID","ITEMID","Budget_date","PurchaseRetailValueatGrossSale","OTBorPurchaseCost","OTBquantity",'budget_amount','budget_cost',"budget_qty"]
        # df = pl.from_pandas(df[columns])
        # sum_cols = [f"pl.col('{col}').sum()" for col in ["PurchaseRetailValueatGrossSale","OTBorPurchaseCost","OTBquantity",'budget_amount','budget_cost',"budget_qty"]]
        
        # agg_dict = [eval(expr) for expr in sum_cols]
        # df = df.groupby(["Channel","INVENTLOCATIONID","ITEMID","Budget_date"]).agg(agg_dict)
        # df = df.rename({'Channel' : 'channel','Budget_date':'BDate','budget_amount':'budget_amount','budget_cost':'BudgetCost','budget_qty':'BudgetQTY'})
        # # return None
        # df =df.to_pandas()
        # df.to_csv(f"{table_name}.tsv",header=True,index=False,sep='\t')
        agg_dict = [eval(expr) for expr in [f"pl.col('{col}').mean()" for col in avg_icols ]
                    + [f"pl.col('{col}').sum()" for col in sumi_cols] +
                     [f"pl.col('{col}').max()" for col in max_icols]]
        print(round(df.estimated_size('gb'),2)," GB memory size of data")

        df = df.group_by(subset).agg(agg_dict)
        print(df.columns, 'colllls')

        df = df.to_pandas()
        print(df.columns.to_list(), 'boooooooo')
        df = df[["ITEMID","Description","Department","Category","Family","SubFamily","SubCategory",
		"ExtendedSubCategory","SubCategorySupplier","AssemblyCodeNickName","ENDOFLife","DOM_COMM","Status",
		"Channel","INVENTLOCATIONID","Store","area",'Region','budget_amount', 'budget_cost','budget_qty',
        "budget_quarter","budget_week", 'budget_month','Budget_date',"Budget_Year",'budget_day',"Supplier",
        'PurchaseRetailValueatGrossSale', 'OTBorPurchaseCost', 'OTBquantity', 'stock_on_hand_qty', 'stock_on_hand_qty_ly',
        'stock_on_hand_qty_lly'
        ]]
        



        df = df.rename(columns=ap_columns)
        # df.head(0).to_sql(table_name, con=db.bind, if_exists='replace', index=False)
        print(df.columns.to_list(), 'sooooooooooo')
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        # print(f"{ctime()} - csv conversion begins")
        # df.to_csv(f"{table_name}.csv",header=True,index=False,sep='\t')
        # print(f"{ctime()} - csv conversion ends")
        # print(f"{ctime()} - writing data to db begins")

        
        
        # with db.bind.raw_connection().cursor() as cursor:
        #     try:
        #         cursor.execute(f"TRUNCATE TABLE {table_name};")
        #         with open(f'{table_name}.csv', 'r',) as file:
        #             cursor.execute("BEGIN;")
        #             cursor.copy_expert(sql = f"COPY {table_name} FROM STDIN WITH DELIMITER E'\t' CSV HEADER;",file=file)
        #         cursor.execute("COMMIT;")
        #     except:
        #         print(traceback.format_exc())

        #         cursor.connection.rollback()
        #     finally:
        #         cursor.close()
        #         os.remove(f'{table_name}.csv')
        # print(f"{ctime()} - writing data to db ends")
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


        df.head(1).to_sql(table_name,con=db.bind,if_exists='replace',index=False,method=None, chunksize=5000)
        db.commit()


    def save_table(self,df:pl.DataFrame,table_name:str,LIBRARY_NAME:str):
        df = df.to_pandas()
        # return None
        STORAGE_URL = 'lmdb://databases/bmaps_data?map_size=50GB'
        rf = RapiDF(STORAGE_URL)
        lib = rf.get_library(LIBRARY_NAME, create_if_missing=True)
        lib.write(table_name, df)
    

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
        
        
        total = len(df)
        average_len = 0       
        df = df.with_columns((pl.lit(0.0)).alias("coefficient_score"))

        secondary_filter = data_filter['secondary_filter']
        if secondary_filter['article_score'] != []:
            try:
                
                for scores in secondary_filter['article_score']:
    #  ARTICLE_SCORES : list = ['sale', 'abc', 'ae', 'speed', 'terminal', 'margin', 'sell', 'markdown', 'core', 'quartile', 'sortimeter']
   
                    if scores == 'sale':
                        average_len += 1
                        df = df.with_columns(((pl.col("coefficient_score") + pl.col("article_score_sale")).replace({np.inf:0,-np.inf:0}).fill_nan(0).fill_null(0)).alias("coefficient_score"))
                    if scores == 'abc':
                        average_len += 1
                        df = df.with_columns(((pl.col("coefficient_score") + pl.col("article_score_abc")).replace({np.inf:0,-np.inf:0}).fill_nan(0).fill_null(0)).alias("coefficient_score"))

                    if scores == 'ae':
                        average_len += 1
                        df = df.with_columns(((pl.col("coefficient_score") + pl.col("article_score_ae")).replace({np.inf:0,-np.inf:0}).fill_nan(0).fill_null(0)).alias("coefficient_score"))

                    if scores == 'speed':
                        average_len += 1
                        df = df.with_columns(((pl.col("coefficient_score") + pl.col("article_score_speed")).replace({np.inf:0,-np.inf:0}).fill_nan(0).fill_null(0)).alias("coefficient_score"))

                    if scores == 'terminal':
                        average_len += 1
                        df = df.with_columns(((pl.col("coefficient_score") + pl.col("article_score_terminal")).replace({np.inf:0,-np.inf:0}).fill_nan(0).fill_null(0)).alias("coefficient_score"))

                    if scores == 'margin':
                        average_len += 1
                        df = df.with_columns(((pl.col("coefficient_score") + pl.col("article_score_margin")).replace({np.inf:0,-np.inf:0}).fill_nan(0).fill_null(0)).alias("coefficient_score"))

                    if scores == 'sell':
                        average_len += 1
                        df = df.with_columns(((pl.col("coefficient_score") + pl.col("article_score_sell")).replace({np.inf:0,-np.inf:0}).fill_nan(0).fill_null(0)).alias("coefficient_score"))

                    if scores == 'markdown':
                        average_len += 1
                        df = df.with_columns(((pl.col("coefficient_score") + pl.col("article_score_markdown")).replace({np.inf:0,-np.inf:0}).fill_nan(0).fill_null(0)).alias("coefficient_score"))

                    if scores == 'core':
                        average_len += 1
                        df = df.with_columns(((pl.col("coefficient_score") + pl.col("article_score_core")).replace({np.inf:0,-np.inf:0}).fill_nan(0).fill_null(0)).alias("coefficient_score"))

                    if scores == 'quartile':
                        average_len += 1
                        df = df.with_columns(((pl.col("coefficient_score") + pl.col("article_score_quartile")).replace({np.inf:0,-np.inf:0}).fill_nan(0).fill_null(0)).alias("coefficient_score"))

                    if scores == 'sortimeter':
                        average_len += 1  
                        df = df.with_columns(((pl.col("coefficient_score") + pl.col("article_score_sortimeter")).replace({np.inf:0,-np.inf:0}).fill_nan(0).fill_null(0)).alias("coefficient_score"))
              
                df = df.with_columns(((pl.col("coefficient_score")/average_len).replace({np.inf:0,-np.inf:0}).fill_nan(0).fill_null(0)).alias("coefficient_score"))
                df = df.with_columns(coefficient_score_mix_percent = (pl.col('coefficient_score')/pl.col('coefficient_score').sum()).replace({-np.inf:0, np.inf:0}).fill_nan(0).fill_null(0)*100)

            except Exception as e:

                print(f"Error:{e}")
                print(traceback.format_exc())

        return df

    
#----------------------------------------------Formulas---------------------------------------------------
# 'Deficit'                         = (('budget_amount')-('final_price')) *('budget_qty')

# SalesActualsByForecast            = (sales_actual/budget_amount)*100

# gross_sales                       =  (budget_amount/(100-Markdown%))

# initial_average_retail_price      = ('History_Net_Sales'/'SALESQTY')* stock_on_hand_qty

# SupplyCost                        = Budget Cost of Goods - (Budget Cost of Goods * Logistics% )

# Original_BudgetAmount             = budget_amount

# StockatRetailPrice                = 'initial_average_retail_price'*stock_on_hand_qty'   

# 'SKU_COUNT'                       = 'ITEMID'

# 'unit_buy_by_sku'                 = 'net sales'/total_sku_count

# 'MarkdownValue'                   = 'gross_sales' - 'budget_amount'

# 'PurchasedRetailValueGrossSale'   =  'budget_amount'/(1-(100-pl.col('ProposedSellThru%')))

# 'budget_qty'                      = 'budget_amount'/'final_price'

# budget_gross_margin_percent       = ('Original_BudgetAmount'*100)-('Original_BudgetCostofGoods')/('Original_BudgetAmount')

# 'OTBorPurchaseCost'               = 'PurchaseRetailValueatGrossSale' * (100 - 'FirstMargin%')

# PurchasedRetailValueGrossSale     = gross_sales * (ProposedSellThru%)/100

# PurchaseRetailValueatGrossSale    = PurchasedRetailValueGrossSale  - TYForecast

# PurchasedRetailValueGrossSale     = budget_amount/(1 - ((100 - ProposedSellThru%)/100))

# PO                                =  budget_amount + (initial_average_retail_price*stock_on_hand_qty)

# budget_gross_margin_percent       = (100*(budget_amount-BudgetCostofGoods))/budget_amount

# COR_valueENDOfLifeStock           = (('StockatRetailPrice)/('StockatRetailPrice.sum())) * newValue












