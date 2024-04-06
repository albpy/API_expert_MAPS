import polars as pl
import numpy as np
import traceback
import subprocess
import time
import pandas as pd
from .parameters import OTB
from .schemas import Filters
from typing import Dict, List, Optional, Union, Tuple

max_col    = OTB.MAX_COLS
avg_col    = OTB.AVG_COLS
sum_col    = OTB.SUM_COLS
float_cols = OTB.FLOAT_COLS
int_cols   = OTB.INT_COLS
HEIRARCHY  = OTB.HEIRARCHY
rank_col   = OTB.RANK_COLS
arts       = OTB.SCORES
cut_cols   = []
OTB = OTB()

class Operations:
#    def edit_tables():

#     return DATA, data 

    def edit_tables(self, DATA : pl.DataFrame, data : pl.DataFrame, row : Dict, group : List, newValue : int, 
    columnID : str, columns_to_filter : List, sub_filter_state : bool, parent : [Optional] = None, child : [Optional] 
    = None,other_filter_condition:[Optional]=None, filter_condition:[Optional]=None) ->pl.DataFrame:
        """
        This function edits tables based on the provided parameters and returns the updated DataFrame.

        Args:
            DATA (pl.DataFrame): The main DataFrame to be edited.
            data (pl.DataFrame): The output DataFrame.
            row (Dict): The selected row from front end.
            group (List): List of selected subfilters columns.
            newValue (int): The new value to be set.
            columnID (str): The ID of the column to be edited.
            columns_to_filter (List): Columns to be used for filtering/hierachical columns.
            sub_filter_state (bool): Sub-filter state , indicating sub_filter selected or not.
            parent: [Optional] = None): Parent filter condition ,series of bool/None.
            child: [Optional] = None): Child filter condition/child location, series of bool/None.
            other_filter_condition: [Optional] = None): Other filter condition/sibling location, series of bool/None.
            filter_condition: [Optional] = None): Filter condition ,series of bool/None..

            Returns:
                pl.DataFrame: The updated DataFrame.

            """
        print(columnID, 'col-id')
        if row[columnID] == None:
                original = 0
        else:
            original  = row[columnID]
        increase = newValue - original 
        if columnID == 'Logistic%':
            # DATA = DATA.with_columns(((pl.col('BudgetCostofGoods')-(pl.col('BudgetCostofGoods')*(pl.col('Logistic%')/100)))).alias('SupplyCost'))
            # DATA = DATA.with_columns((((pl.col('budget_amount')-pl.col('SupplyCost'))*100)/pl.col('budget_amount')).alias('FirstMargin%'))
            # DATA = DATA.with_columns(((pl.col('PurchaseRetailValueatGrossSale')*((100-pl.col('FirstMargin%'))))).alias('OTBorPurchaseCost'))
            print("Logistic column is edited")
            try:              

                # DATA['Logistic%'].loc[child] = newValue
                if child is None:
                    # print(parent.value_counts(), 'parent_logistics')
                    print('Logistic child is none')
                    DATA = DATA.with_columns((pl.lit(newValue).cast(pl.Float64)).alias('Logistic%'))
                    
                    DATA = DATA.with_columns(((pl.col('BudgetCostofGoods')-(pl.col('BudgetCostofGoods')*(pl.col('Logistic%')/100)))).alias('SupplyCost'))
                    DATA = DATA.with_columns((((pl.col('budget_amount')-pl.col('SupplyCost'))*100)/pl.col('budget_amount')).alias('FirstMargin%'))
                    DATA = DATA.with_columns(((pl.col('PurchaseRetailValueatGrossSale')*((100-pl.col('FirstMargin%'))))).alias('OTBorPurchaseCost'))
                else:
                    data_child = DATA.filter(list(child))
                    data_child = data_child.with_columns((pl.lit(newValue).cast(pl.Float64)).alias('Logistic%'))
                    data_child = data_child.with_columns(((pl.col('BudgetCostofGoods')-(pl.col('BudgetCostofGoods')*(pl.col('Logistic%')/100)))).alias('SupplyCost'))
                    data_child = data_child.with_columns((((pl.col('budget_amount')-pl.col('SupplyCost'))*100)/pl.col('budget_amount')).alias('FirstMargin%'))
                    data_child = data_child.with_columns(((pl.col('PurchaseRetailValueatGrossSale')*((100-pl.col('FirstMargin%'))))).alias('OTBorPurchaseCost'))
                    
                    data_other = DATA.filter(list(child.not_()))

                    DATA = pl.concat([data_child, data_other])

#                     
            except Exception as e:
                print(f"Error Logistic_p is: {e}")
                # DATA['Logistic%'] = newValue
                DATA = DATA.with_columns(pl.lit(newValue).alias('Logistic%'))

        if columnID == 'DisplayItemQty':
            try:
                # DATA['DisplayItemQty'].loc[child] = drill_down_display(DATA.loc[child],columnID,newValue)
                DATA_child = DATA.filter(list(child))
                DATA_non_child = DATA.filter(list(child.not_()))
                print(OTB.drill_down_display(DATA.filter(list(child)),columnID,newValue), 'drill_dw_dis of DisplayItem')
                DATA_child = DATA_child.with_columns(DisplayItem = OTB.drill_down_display(DATA.filter(list(child)),columnID,newValue))
                DATA = pl.concat([DATA_child,DATA_non_child])
                print(DATA, "Concated DisplayItem")

                # try:
                #     DATA['DisplayItemQty'].loc[child] = newValue
                # except:
                #     DATA['DisplayItemQty'] = newValue
            except Exception as e:

                print(f"Error DisplayItem is: {e}")
                # DATA = DATA.with_columns(DisplayItem = OTB.drill_down_display(DATA,columnID,newValue))
                DATA = DATA.with_columns(DisplayItem = (pl.col('initial_average_retail_price')/pl.col('initial_average_retail_price').sum()).replace({np.inf:0,-np.inf:0}).fill_nan(0)*newValue)

                # DATA['DisplayItemQty'].value_counts().write_csv('stretpr.csv')
                # print(DATA.select(pl.col(['initial_average_retail_price', 'DisplayItemQty']).sum()), newValue, 'xxxxxx')
                print(DATA['DisplayItemQty'].sum(), 'Disscplay')
                # print(DATA['DisplayItemQty'].value_counts())
        # if columnID == 'adjusted_budget_gross_margin_percent':
        #     DATA['adjusted_budget_gross_margin_percent'].loc[child] = newValue
        
        if columnID == 'COR_EOLStock_value':
            # try:
            # print(child,'child')
                            
            # DATA['COR_EOLStock_value'].loc[child] = drill_down_cor(DATA.loc[child],columnID,newValue)
            data1 = DATA.group_by(list(set(['Channel'])))#.agg(agg_dict) # ,  maintain_order = True
            for name, data in data1:
                print(name)
                print(data.select(pl.col(['Channel', 'ProposedSellThru%', 'PurchaseRetailValueatGrossSale', 'TYForecast', 'OTBorPurchaseCost', 'PurchasedRetailValueGrossSale'])), 'corr')
                print(data.select(pl.col(['Channel', 'ProposedSellThru%', 'PurchaseRetailValueatGrossSale', 'TYForecast', 'OTBorPurchaseCost', 'PurchasedRetailValueGrossSale']).sum()), 'corr')
           
            try:
                print(DATA['COR_EOLStock_value'].filter(list(child)), 'ccch')
                
                DATA_child = DATA.filter(list(child))
                DATA_non_child = DATA.filter(list(child.not_()))
                DATA_child = DATA_child.with_columns(StockatRetailPrice = pl.col('StockatRetailPrice').fill_null(0).replace({np.inf:0,-np.inf:0}).fill_nan(0).cast(pl.Float64))
                # df = df.with_columns(columnID = pl.col('StockatRetailPrice').cast(pl.Float64)/pl.col('StockatRetailPrice').cast(pl.Float64).fill_nan(0).sum()) * int(newValue)
                DATA_child = DATA_child.with_columns((((pl.col('StockatRetailPrice')/pl.col('StockatRetailPrice').sum())).replace({np.inf:0,-np.inf:0}).fill_nan(0).cast(pl.Float64) * newValue).alias(columnID))

                # DATA_child = DATA_child.with_columns(COR_EOLStock_value = OTB.drill_down_cor(DATA.filter(list(child)),columnID,newValue))
                # print(OTB.drill_down_cor(DATA.filter(list(child)),columnID,newValue), 'drill_dw_cor of DisplayItem')
                DATA_non_child = DATA_non_child.with_columns(COR_EOLStock_value = pl.col('COR_EOLStock_value').cast(pl.Float64))
                DATA = pl.concat([DATA_child,DATA_non_child])
                print(DATA, 'Concated COR_EOLStock_value')
            # print(DATA['COR_EOLStock_value'].loc[child],'TABLEDATA')
            
            # print(DATA['COR_EOLStock_value'].sum(),'DATAFiltercor')
            # except:
            #     DATA['COR_EOLStock_value'] = newValue
            except Exception as e:
                print('Exception CoRR', e, traceback.format_exc())#.replace({np.inf:0,-np.inf:0}).fill_nan(0).cast(pl.Int32) * newValue
                DATA = DATA.with_columns((((pl.col('StockatRetailPrice')/pl.col('StockatRetailPrice').sum()).replace({np.inf:0,-np.inf:0}).fill_nan(0)) * newValue).alias(columnID))
                print(newValue, 'news')
                # DATA['COR_EOLStock_value'].value_counts().write_csv('stretpr.csv')
                # DATA = DATA.with_columns((pl.lit(newValue)).alias('COR_EOLStock_value'))
                print(DATA[columnID].sum(), DATA['StockatRetailPrice'].sum(), 'sto,cor')
                # print(f"Error COR_EOLStock_value is: {e}")
        if columnID == 'Markdown%':
            try:
                # DATA['Markdown%'].loc[child] = newValue
                DATA_child = DATA.filter(list(child))
                print("Data_child", DATA_child)
                DATA_child_other = DATA.filter(list(child.not_()))
                print("Data_Child_not", DATA_child_other)
                DATA_child = DATA_child.with_columns((pl.lit(newValue).cast(pl.Float64)).alias('Markdown%'))
                print(DATA_child, "ADded DATA markdown")
                DATA = pl.concat([DATA_child, DATA_child_other])
                print(DATA, 'Markdown concated data')
            except Exception as e:
                print(f"Error Markdown is: {e}")
                # DATA['Markdown%'] = newValue
                DATA = DATA.with_columns(pl.lit(newValue).alias('Markdown%'))
                print(DATA, "Markdown except %")
        if columnID == 'ProposedSellThru%':
            # print(DATA['initial_average_retail_price'].sum(), 'init_av_ret_price')
            # print(DATA['PurchaseRetailValueatGrossSale'].is_null().sum(), 'PurchaseRetailValueatGrossSale')
            # print(DATA['PurchasedRetailValueGrossSale'].is_null().sum(), 'PurchaseRetailValueGrossSale')
            
            try:
                # DATA['ProposedSellThru%'].loc[child] = newValue
                data_child = DATA.filter(list(child))
                data_other = DATA.filter(list(child.not_()))
                data_child = data_child.with_columns((pl.lit(newValue).cast(pl.Float64)).alias('ProposedSellThru%'))
                
                data_child =data_child.with_columns(PurchasedRetailValueGrossSale = pl.col('budget_amount')/(1-((100-pl.col('ProposedSellThru%'))/100)))
                data_child =data_child.with_columns(PurchaseRetailValueatGrossSale = pl.col('PurchasedRetailValueGrossSale')-pl.col('TYForecast'))
                data_child =data_child.with_columns(OTBorPurchaseCost = pl.col('PurchaseRetailValueatGrossSale')*((100-pl.col('FirstMargin%'))))
                # print(set(data_child.columns)-set(data_other.columns),'yyyy')
                # print(set(data_other.columns)-set(data_child.columns))
                DATA = pl.concat([data_child,data_other])
                print(DATA, 'Concated ProposedSellThru%')

            except Exception as e:
                print(f"Error ProposedSellThru is: {e}")
                # DATA['ProposedSellThru%'] = newValue
                DATA = DATA.with_columns(pl.lit(newValue).alias('ProposedSellThru%'))
                # DATA.with_columns(newValue = pl.col('ProposedSellThru%'))
                print(DATA, "ProposedSellThru except")
                

        if columnID == 'act_forecast_vs_budget_percent':
            print(f'Column_id is:: {columnID}')
            print(newValue, 'newvalue')

            budget_amount = (newValue*row['ACT_FCT'])/100
            print(budget_amount, 'New_bud_amnt')

            print(child.value_counts(), 'child_act_fct')
            print(other_filter_condition.value_counts(), 'other_act_fct')
            print('There is parent::', parent) 
            budget_amount_sum = DATA['budget_amount'].sum()
            # newValue = (DATA.loc[child]['RelativeBudget%'].sum()*budget_amount)/DATA.loc[child]['budget_amount'].sum()
            newValue = (100*budget_amount)/budget_amount_sum
            columnID = 'budget_percent'
            data_child = DATA.filter(list(child))
            data_other = DATA.filter(list(child.not_()))
            # DATA = OTB.change_percent(grouped_df=DATA.filter(list(child)), other_grouped_df=DATA.filter(list(other_filter_condition)), increase=increase, colID=columnID)

            # data_child = data_child.with_columns((pl.lit(newValue).cast(pl.Float64)).alias('act_forecast_vs_budget_percent'))
            # data_child = data_child.with_columns(((pl.col('act_forecast_vs_budget_percent')*pl.col('sales_actual'))/100).alias('budget_amount')) 
            # data_child = data_child.with_columns(((pl.col('budget_amount')/pl.col('budget_amount').sum())*100).alias('budget_percent'))

            # DATA = pl.concat([data_child, data_other])
            
            # print(DATA.select(pl.all().is_null().sum()).to_dicts()[0], 'print the nulls od DATA after edited')
            # print(DATA['act_forecast_vs_budget_percent'].value_counts(), 'the data after agg')

        if columnID == 'budget_qty':
            # budget_amount = newValue*row['Price']
            budget_amount = newValue*row['initial_average_retail_price']
            if parent is None: budget_amount_sum = DATA['budget_amount'].sum()
            # else: budget_amount_sum = DATA.loc[parent]['budget_amount'].sum()
            else:
                budget_amount_DATA = DATA.filter(list(parent))
                budget_amount_sum = budget_amount_DATA['budget_amount'].sum()
            if budget_amount < budget_amount_sum:
                newValue = (100*budget_amount)/budget_amount_sum
                columnID = 'budget_percent'
        if columnID == 'budget_vpy':
            budget_amount = newValue*row['History_Net_Sales']
            if parent is None: budget_amount_sum = DATA['budget_amount'].sum()
            # else: budget_amount_sum = DATA.loc[parent]['budget_amount'].sum()
            else:
                budget_amount_DATA = DATA.filter(list(parent))
                budget_amount_sum = DATA['budget_amount'].sum()
            if budget_amount < budget_amount_sum:
                newValue = (100*budget_amount)/budget_amount_sum
                columnID = 'budget_percent'
        if columnID == 'adjusted_budget_gross_margin_percent':
            budget_amount =row['BudgetCostofGoods']/(1- 0.01*newValue)
            if parent is None: 
                print('parent is', parent)
                print(DATA, 'DATA______')
                budget_amount_sum = DATA['budget_amount'].sum()
            else: 
                print('There is parent is:', parent)
                # budget_amount_sum = DATA.loc[parent]['budget_amount'].sum()
                budget_amount_DATA = DATA.filter(list(parent))
                budget_amount_sum = DATA['budget_amount'].sum()
            if budget_amount < budget_amount_sum:
                print('budget_amount < budget_amount_sum')
                newValue = (100*budget_amount)/budget_amount_sum
                columnID = 'budget_percent'

        # if columnID == 'Adjusted Budget Gross Margin%':
        #     DATA.loc[child] = drill_down_percentage(DATA.loc[child],'Adjusted Budget Gross Margin%',newValue,'Budget Gross Margin%')

        if columnID == 'budget_percent':
            # s = 0.01*(newValue)*(row['budget_amount']/(row['budget_percent']*0.01))
            # print(s)
            # increase = newValue -s
            # s = data['RelativeBudget%'].sum()
            s = data['relative_budget_percent'].sum()
            increase = (s*newValue/100) - (s*row[columnID]/100)
            print('parent is bud_per', parent)
            print('child is bud_per', child)
            # if child == None:
            #     print('parent is None bud_p')
            #     DATA = DATA.with_columns(budget_percent = (DATA["budget_percent"] * newValue)/100)
            #     # agg_dict = [eval(expr) for expr in 
            #     #         [f"pl.col('{col}').mean()" for col in avg_col]+[f"pl.col('{col}').sum()" for col in sum_col]]
            #     # data = DATA.select(agg_dict)
            
            # else:
            if len(columns_to_filter) == 1:
                print('len(columns_to_filter)==1')
                # DATA.loc[child], DATA.loc[other_filter_condition] = OTB.distribute_value(child_df=DATA.loc[child],siblings_df=DATA.loc[other_filter_condition],increase=increase,columnID =columnID)
                # DATA = change_percent(grouped_df=DATA.loc[child],other_grouped_df=DATA.loc[other_filter_condition],increase= increase, colID= columnID)
                DATA = OTB.change_percent(grouped_df=DATA.filter(list(child)), other_grouped_df=DATA.filter(list(other_filter_condition)), increase=increase, colID=columnID)
            else:
                DATA_parent = OTB.change_percent(grouped_df = DATA.filter(list(child)), other_grouped_df = DATA.filter(list(other_filter_condition)), increase = increase, colID = columnID)
                DATA_siblings = DATA.filter(list(parent.not_()))
                DATA = pl.concat([DATA_siblings,DATA_parent])
        elif columnID not in ['Logistic%','DisplayItemQty','adjusted_budget_gross_margin_percent','COR_EOLStock_value','Markdown%','ProposedSellThru%', 'Check_box']:
            print(columnID)
            if len(columns_to_filter) == 1:
                print('len of col to filter is:', 1)
                # DATA = change_value(grouped_df=DATA.loc[child],other_grouped_df=DATA.loc[other_filter_condition],increase= increase, colID= columnID)
                DATA = OTB.change_percent(grouped_df=DATA.filter(list(child)), other_grouped_df=DATA.filter(list(other_filter_condition)), increase=increase, colID=columnID)

            else:
                print('len of col to filter is:', len(columns_to_filter))
                # DATA.loc[parent] = change_value(grouped_df=DATA.loc[child],other_grouped_df=DATA.loc[other_filter_condition],increase= increase, colID= columnID)
                DATA = OTB.change_value(grouped_df=DATA.filter(list(child)),other_grouped_df=DATA.filter(list(other_filter_condition)),increase= increase, colID= columnID)

        if columnID == 'budget_percent':
            summation = DATA['budget_amount'].sum()
            # DATA['budget_amount'] = DATA[columnID]*summation*0.01
            DATA = DATA.with_columns(budget_amount = DATA[columnID]*summation/100)

            # DATA['BudgetQTY'] = (DATA['budget_amount']/DATA['Price']).astype(float).round().astype(int)
            DATA = DATA.with_columns(((pl.col("budget_amount")/pl.col("initial_average_retail_price")).round()).alias('budget_qty'))

            # DATA['Deficit'] = DATA['budget_amount'] - (DATA['Price']*DATA['BudgetQTY'])
            DATA = DATA.with_columns(((pl.col('budget_amount')-(pl.col('initial_average_retail_price')*pl.col('budget_qty')))).alias('Deficit'))
            # DATA['RelativeBudget%'] = 100*DATA['budget_amount']/DATA['budget_amount'].sum()
            DATA = DATA.with_columns(((DATA['budget_amount']/DATA['budget_amount'].sum())*100).alias("relative_budget_percent"))

            # DATA['Budget vs ACT/FCT'] = (100*DATA['budget_amount'])/DATA['ACT_FCT']
           #------------------ 
            # DATA = DATA.with_columns(((pl.col('budget_amount')/pl.col("sales_actual")).replace({np.inf:0,-np.inf:0}).fill_nan(0)*100).alias("act_forecast_vs_budget_percent"))
           #-----------------------
            # DATA['adjusted_budget_gross_margin_percent'] = ((DATA['budget_amount']-DATA['BudgetCostofGoods'])/DATA['budget_amount'])*100
            DATA = DATA.with_columns((((pl.col('budget_amount') - pl.col('BudgetCostofGoods'))/pl.col('budget_amount'))*100).alias('adjusted_budget_gross_margin_percent'))
            # DATA['BudgetCostofGoods'] = (DATA['budget_amount']-(DATA['budget_amount']*DATA['adjusted_budget_gross_margin_percent'])/100)
            DATA = DATA.with_columns(((pl.col('budget_amount')-(pl.col('budget_amount')*pl.col('adjusted_budget_gross_margin_percent'))/100)).alias('BudgetCostofGoods'))
            # DATA['FirstMargin%'] = 100*(DATA['budget_amount']-DATA['SupplyCost'])/DATA['budget_amount']
            DATA = DATA.with_columns((100*(pl.col('budget_amount')-pl.col('SupplyCost'))/pl.col('budget_amount')).alias('FirstMargin%'))
            ### # DATA['Markdown%'] = (DATA['GrossSales'] - DATA['budget_amount'])/DATA['GrossSales']
            # DATA['GrossSales']         =  DATA['budget_amount']/(100-DATA['Markdown%'])
            DATA = DATA.with_columns(((pl.col('budget_amount')/(100-pl.col('Markdown%'))).alias('GrossSales')))
            # DATA['PurchasedRetailValueGrossSale'] = DATA['GrossSales'] * (DATA['ProposedSellThru%'])/100
            DATA = DATA.with_columns(((pl.col('GrossSales')*pl.col('ProposedSellThru%'))).alias('PurchasedRetailValueGrossSale'))
            # DATA['PurchaseRetailValueatGrossSale']= DATA['PurchasedRetailValueGrossSale']   - DATA['TYForecast']
            DATA = DATA.with_columns((pl.col('PurchasedRetailValueGrossSale')-pl.col('TYForecast')).alias('PurchaseRetailValueatGrossSale'))
            # DATA['OTBorPurchaseCost'] = DATA['PurchaseRetailValueatGrossSale'] * ((100 - DATA['FirstMargin%']))/100
            DATA = DATA.with_columns(((pl.col('PurchaseRetailValueatGrossSale')*(100 - pl.col('FirstMargin%')))).alias('OTBorPurchaseCost'))

    #    elif columnID == 'Logistic%':
            # DATA['SupplyCost'] = DATA['BudgetCostofGoods'] - (DATA['BudgetCostofGoods']*(DATA['Logistic%']/100))
            # DATA['FirstMargin%'] = 100*(DATA['budget_amount']-DATA['SupplyCost'])/DATA['budget_amount']
            # DATA['OTBorPurchaseCost'] = DATA['PurchaseRetailValueatGrossSale'] * ((100 - DATA['FirstMargin%']))/100
            
            # DATA = DATA.with_columns(((pl.col('BudgetCostofGoods')-(pl.col('BudgetCostofGoods')*(pl.col('Logistic%')/100)))).alias('SupplyCost'))
            # DATA = DATA.with_columns((((pl.col('budget_amount')-pl.col('SupplyCost'))*100)/pl.col('budget_amount')).alias('FirstMargin%'))
            # DATA = DATA.with_columns(((pl.col('PurchaseRetailValueatGrossSale')*((100-pl.col('FirstMargin%'))))).alias('OTBorPurchaseCost'))
        
        

        elif columnID == 'Markdown%':
            
            # DATA['GrossSales']         =  (DATA['budget_amount']/(100-DATA['Markdown%'])).fillna(0)*100
            DATA =DATA.with_columns(GrossSales = (pl.col('budget_amount')/(100-pl.col('Markdown%'))).fill_nan(0))
            # DATA['MarkdownValue']      = DATA['GrossSales'] - DATA['budget_amount']
            DATA =DATA.with_columns(MarkdownValue = (pl.col('GrossSales')-pl.col('budget_amount')))
            # DATA['PurchasedRetailValueGrossSale'] = DATA['GrossSales'] * (DATA['ProposedSellThru%'])/100
            # DATA.with_columns(PurchasedRetailValueGrossSale = (pl.col('GrossSales')*pl.col('ProposedSellThru%'))/100)
            DATA = DATA.with_columns(PurchasedRetailValueGrossSale = ((pl.col('budget_amount')/(100-pl.col('Markdown%')))/(pl.col('ProposedSellThru%')/100)).replace({np.inf:0, -np.inf:0}).fill_nan(0).fill_null(0))

            # DATA['PurchaseRetailValueatGrossSale']= DATA['PurchasedRetailValueGrossSale']   - DATA['TYForecast']
            DATA =DATA.with_columns(PurchaseRetailValueatGrossSale = pl.col('PurchasedRetailValueGrossSale')-pl.col('TYForecast'))
            # DATA['OTBorPurchaseCost'] = DATA['PurchaseRetailValueatGrossSale'] * ((100 - DATA['FirstMargin%']))/100
            DATA =DATA.with_columns(OTBorPurchaseCost = (pl.col('PurchasedRetailValueGrossSale')*(100-pl.col('FirstMargin%'))))
        
        # elif columnID == 'ProposedSellThru%':

        #     # DATA['PurchasedRetailValueGrossSale'] = DATA['GrossSales'] * DATA['ProposedSellThru%']
        #     # DATA['GrossSales'] = DATA['PurchasedRetailValueGrossSale']/DATA['ProposedSellThru%']
        #     # DATA['Markdown%'] = (DATA['GrossSales'] - DATA['budget_amount'])/DATA['GrossSales']
        #     # DATA['PurchasedRetailValueGrossSale'] = DATA['budget_amount'] / (DATA['ProposedSellThru%'].replace(0,pd.NA)/100)
        #     #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        #     # DATA['PurchasedRetailValueGrossSale'] = DATA['budget_amount'] / (1 - ((100 - data['ProposedSellThru%']) / 100))
        #     DATA =DATA.with_columns(PurchasedRetailValueGrossSale = pl.col('budget_amount')/(1-((100-pl.col('ProposedSellThru%'))/100)))
        #     # DATA['PurchaseRetailValueatGrossSale']= DATA['PurchasedRetailValueGrossSale']   - DATA['TYForecast']
        #     DATA =DATA.with_columns(PurchaseRetailValueatGrossSale = pl.col('PurchasedRetailValueGrossSale')-pl.col('TYForecast'))
        #     # DATA['OTBorPurchaseCost'] = DATA['PurchaseRetailValueatGrossSale'] * ((100 - DATA['FirstMargin%']))/100
        #     DATA =DATA.with_columns(OTBorPurchaseCost = pl.col('PurchaseRetailValueatGrossSale')*((100-pl.col('FirstMargin%'))))
            
        elif columnID == 'DisplayItemQty':
            # DATA = DATA.with_columns((pl.col('DisplayItemQty') * pl.col('initial_average_retail_price')).alias('DisplayItemValue'))
            DATA = DATA.with_columns(initial_average_retail_price = pl.col('initial_average_retail_price').replace({np.inf:0,-np.inf:0}).fill_nan(0))
            # DATA = DATA.with_columns(pl.col())
            DATA = DATA.with_columns(DisplayItemValue = pl.col('DisplayItemQty')*pl.col('initial_average_retail_price'))
            # DATA['TYForecast'] = DATA['StockatRetailPrice'] - DATA['DisplayItemValue'] - DATA['COR_EOLStock_value']
            DATA = DATA.with_columns(TYForecast = pl.col('StockatRetailPrice')- pl.col('DisplayItemValue')-pl.col('COR_EOLStock_value'))
            # print(DATA.select(pl.col(['StockatRetailPrice', 'DisplayItemValue', 'COR_EOLStock_value']).sum()), 'st__disp')
            # DATA['PurchaseRetailValueatGrossSale'] = DATA['PurchasedRetailValueGrossSale'] - DATA['TYForecast']
            DATA = DATA.with_columns(PurchaseRetailValueatGrossSale = pl.col('PurchasedRetailValueGrossSale')-pl.col('TYForecast'))
            # DATA['OTBorPurchaseCost'] = DATA['PurchaseRetailValueatGrossSale'] * ((100 - DATA['FirstMargin%']))/100
            DATA = DATA.with_columns(OTBorPurchaseCost=pl.col('PurchaseRetailValueatGrossSale')*((100-pl.col('FirstMargin%'))))
            
        elif columnID == 'COR_EOLStock_value':
            try:
                # if child:
                print('we are here')
                # DATA['TYForecast'] = DATA['StockatRetailPrice']- DATA['DisplayItemValue']- DATA['COR_EOLStock_value'].astype(float)
                DATA = DATA.with_columns(TYForecast = pl.col('StockatRetailPrice')-pl.col('DisplayItemValue')-pl.col('COR_EOLStock_value').cast(pl.Float64))
                print(DATA.select(pl.col(["TYForecast", 'StockatRetailPrice', 'DisplayItemValue', 'COR_EOLStock_value']).sum()), 'TYgaa')
                # data['PurchasedRetailValueGrossSale']  = data['PurchaseRetailValueatGrossSale']+data['TYForecast']
                # DATA = DATA.with_columns(PurchasedRetailValueGrossSale = pl.col('PurchaseRetailValueatGrossSale')+pl.col('TYForecast'))
                DATA = DATA.with_columns(PurchasedRetailValueGrossSale = (pl.col('gross_sales') * (pl.col('ProposedSellThru%'))/100).replace({np.inf:0, -np.inf:0}).fill_nan(0).fill_null(0))

                # DATA['PurchaseRetailValueatGrossSale'] = DATA['PurchasedRetailValueGrossSale'] - DATA['TYForecast']
                # DATA = DATA.with_columns(PurchaseRetailValueatGrossSale = pl.col('PurchasedRetailValueGrossSale')-pl.col('TYForecast'))
                DATA = DATA.with_columns((((pl.col('PurchasedRetailValueGrossSale'))-pl.col('TYForecast')).replace({np.inf:0, -np.inf:0}).fill_nan(0).fill_null(0)).alias('PurchaseRetailValueatGrossSale'))

                # DATA['OTBorPurchaseCost'] = DATA['PurchaseRetailValueatGrossSale'] * ((100 - DATA['FirstMargin%']))/100
                DATA = DATA.with_columns(OTBorPurchaseCost = pl.col('PurchaseRetailValueatGrossSale')-((100-pl.col('FirstMargin%'))))
                # print(DATA.select(pl.col(['PurchaseRetailValueatGrossSale', 'FirstMargin%', 'OTBorPurchaseCost']).sum()), 'pooo')
                DATA = DATA.with_columns((((pl.col('PurchaseRetailValueatGrossSale'))/(pl.col('initial_average_retail_price'))).replace({np.inf:0, -np.inf:0}).fill_nan(0)).alias('OTBquantity'))
                # print(DATA.select(pl.col(['PurchaseRetailValueatGrossSale', 'initial_average_retail_price', 'OTBquantity', ]).sum()), 'OTB__quantity')

                # data1 = DATA.group_by(list(set(['Channel'])))#.agg(agg_dict) # ,  maintain_order = True
                # for name, data in data1:
                #     print(name)
                #     print(data.select(pl.col(['Channel', 'ProposedSellThru%', 'PurchaseRetailValueatGrossSale', 'TYForecast', 'OTBorPurchaseCost', 'PurchasedRetailValueGrossSale'])), 'corrrrr')
                #     print(data.select(pl.col(['Channel', 'ProposedSellThru%', 'PurchaseRetailValueatGrossSale', 'TYForecast', 'OTBorPurchaseCost', 'PurchasedRetailValueGrossSale']).sum()), 'corrrrr')
            except:
                print('crrrrel', traceback.format_exc())
        return DATA, data
    
    def apply_group_by(self, DATA : pl.DataFrame, data : pl.DataFrame, data_filter : dict, sub_filter_state : bool, group : List, filters : Filters, filter_condition : [Optional] = None)->Union[None,pl.Series,pl.DataFrame]:

        """
            Apply group-by operation on the given DataFrame based on specified parameters.

            Args:
                DATA (pl.DataFrame): The main DataFrame.
                data_filter (Dict): input filter response.
                sub_filter_state (bool): Sub-filter state/ subfilter selected or not.
                group (List): List of columns used for grouping/subfilter columns.
                filters (Filters): Filters object.
                filter_condition: filter condition for main data /series of bool.

            Returns:
                Union[None, pl.Series, pl.DataFrame]: The result of the group-by operation filter_condtion and dataframe.

        """
        print(DATA.columns, 'data cols before agg')
        print('we are finding corr gp0')

        filter_condition,sub_filter_state,group = OTB.secondary_filter(DATA,filters,sub_filter_state,group,filter_condition)
        if filter_condition is not None:
            # Add filter_index column based on filter_condition
            DATA = DATA.with_columns(filter_index=filter_condition)
            DATA = DATA.with_columns(filter_index= pl.col("filter_index").cast(pl.Int8))

        # data1 = DATA.group_by(list(set(['Channel'])))#.agg(agg_dict) # ,  maintain_order = True
        # for name, data in data1:
        #     print(name)
        #     print(data['COR_EOLStock_value'].sum(), 'ggggggg')
        secondary = data_filter['secondary_filter']
        scores_m = secondary['article_score']
        print(scores_m, 'scoresss')
        if not len(data_filter['group_by']['columns']) == 0:
            print('>_0_len_g')
            for i in  data_filter['group_by']['columns']:
                if i in OTB.filter_details:
                    group.append(OTB.filter_details[i])
            
            limit = max_col.index(group[-1])
            
            mean_cols = [f"pl.col('{col}').mean()" for col in avg_col if col!= 'total_sku_count']
            sum_cols =  [f"pl.col('{col}').sum()" for col in sum_col ] # if col!= 'total_sku_count'
            rank_cols = [f"pl.col('{col}').max()" for col in rank_col]
            dist_cols = [f"pl.col('{col}').sum()" for col in ['new_budget_mix', 'revised_budget_amount'] if col in DATA.columns]
            art_cols = [f"pl.col('{col}').mean()" for a, col in arts.items() if a in scores_m]
            cumulatives_1 = [f"pl.col('{col}').mean()" for col in ['coefficient_score'] if len(art_cols)!=0]
            cumulatives_2 = [f"pl.col('{col}').sum()" for col in ['coefficient_score_mix_percent'] if len(art_cols)!=0]
            
            cleaned_data = DATA.select(pl.col(sum_col).sum())
            print(cleaned_data, 'cleansed_dataaa')
            # cut_cols = cut_cols.append([col for col in DATA.columns if (DATA[col]==0).all()])
            # print(cut_cols, 'cuttercols')

            print(art_cols, 'art_colsssss')
            # uniques =  [f"pl.col('{col}').n_unique()" for col in ['total_sku_count']]
            agg_dict = [eval(expr) for expr in mean_cols+sum_cols+rank_cols+dist_cols+art_cols+cumulatives_1+cumulatives_2] #+uniques
            
            if filter_condition is not None:
                print("filter_cond_ex_g")
                try:
                    filter_condition = DATA["filter_index"]==1
                    data = DATA.filter(list(filter_condition)).group_by(group).agg(agg_dict) #, maintain_order = True
#--------------------------------------------------------------------------call_filter------------------------------------------------------------------
                    OTB.SUB_FILTER = OTB.call_filter(DATA.filter(list(filter_condition)), OTB.SUB_FILTER, group, DATA)
                except Exception as e:
                    print(f'error of grpby stts none fl_condition is {e}')
                    pass
            else:
                try:
                    print("no_filter_g")
                    print(group, 'grp_g')
                    #----------------------------------Primary filter------------------------------------------
                    # data = OTB.main_filters(DATA, data_filter, sub_filter_state,group,filter_condition)
                    #------------------------------------------------------------------------------------------
                    data = DATA.group_by(list(set(group))).agg(agg_dict) #, maintain_order = True
                    print(data, 'main_fil_grouped')
                except Exception as e:
                    # print(traceback.format_exec())
                    print(group)
                    print(f"Error in grpby sttsxxx with cols is {e}")           
        else:
            #---
            print('0_g_len')
        
            mean_cols = [f"pl.col('{col}').mean()" for col in avg_col]
            sum_cols = [f"pl.col('{col}').sum()" for col in sum_col ] # if col != 'total_sku_count'
            # uniques = [f"pl.col('{col}').n_unique()" for col in ['total_sku_count']]
            rank_cols = [f"pl.col('{col}').max()" for col in rank_col]
            dist_cols = [f"pl.col('{col}').sum()" for col in ['new_budget_mix', 'revised_budget_amount'] if col in DATA.columns]
            # uniques =  [f"pl.col('{col}').n_unique()" for col in ['total_sku_count']]
            art_cols = [f"pl.col('{col}').mean()" for a, col in arts.items() if a in scores_m]
            cumulatives_1 = [f"pl.col('{col}').mean()" for col in ['coefficient_score'] if len(art_cols)!=0]
            cumulatives_2 = [f"pl.col('{col}').sum()" for col in ['coefficient_score_mix_percent'] if len(art_cols)!=0 ] 
            # print(art_cols, 'AAAAArrtttt')
            
            agg_dict = [eval(expr) for expr in mean_cols+sum_cols+rank_cols+dist_cols+art_cols+cumulatives_1+cumulatives_2]  #+uniques            print('total cols is: ', agg_dict )
        
            if not sub_filter_state:
                print('sub_filter_state_0_g')
                try:
                    # Apply group-by without filter conditions
                    data = DATA.select(agg_dict)
                except Exception as e:
                    print(f"Error in grpby status without cols, S_F_S is {e}")
                    print(traceback.format_exc())
            else:
                try:
                    print('we are here, sub fil state')
                    filter_condition = DATA["filter_index"]==1
                    print(filter_condition, 'error causing column')
                    print(group)
                    print(DATA.columns, 'check sfs col')
                    data = DATA.filter(list(filter_condition)).group_by(list(set(group)), maintain_order = True).agg(agg_dict) #, maintain_order = True
                    # print(data['budget_quarter'])
                    print(data, 'check subsfils')
                    # print(data, 'ffffffffffff')
                    OTB.SUB_FILTER = OTB.call_filter(DATA.filter(list(filter_condition)), OTB.SUB_FILTER, group, DATA)
                except Exception as e:
                    print(f"Error in grpby sttsxxx withot cols, with S_F_S is {e}")
                    pass
            print(data.columns, 'cccccccoooolllls')
        return data, filter_condition

    def expand_hierarchy(self, DATA : pl.DataFrame, data_filter : Dict, sub_filter_state : bool, group : List, filters : Filters, filter_condition : [Optional] = None) -> Union[None, pl.Series, pl.DataFrame]: 
        """
        Expand/contract the hierarchy of the given DataFrame based on specified parameters.

        Args:
            DATA (pl.DataFrame): The main DataFrame.
            data_filter (Dict): Filter response from front end.
            sub_filter_state (bool): Sub-filter state/ flag indicating subfilter selected or not.
            group (List): List of columns used for grouping/subfilter columns.
            filters (Filters): Filters object.
            filter_condition: [Optional] = None): filter condition for main data /series of bool.

        Returns:
            Union[None, pl.Series, pl.DataFrame]: The main data,
            The result of expandad/contracted dataframe as per the selection and filter_condtion .

        """
        
        print('we are finding corr ex1')
        secondary = data_filter['secondary_filter']
        scores_m = secondary['article_score']
        mean_cols = [f"pl.col('{col}').mean()" for col in avg_col]
        sums_cols = [f"pl.col('{col}').sum()" for col in sum_col]
        rank_cols = [f"pl.col('{col}').max()" for col in rank_col]
        dist_cols = [f"pl.col('{col}').sum()" for col in ['new_budget_mix', 'revised_budget_amount'] if col in DATA.columns]
        art_cols = [f"pl.col('{col}').mean()" for a, col in arts.items() if a in scores_m]
        cumulatives_1 = [f"pl.col('{col}').mean()" for col in ['coefficient_score'] if len(art_cols)!=0]
        cumulatives_2 = [f"pl.col('{col}').sum()" for col in ['coefficient_score_mix_percent'] if len(art_cols)!=0]
    
        # uniques =  [f"pl.col('{col}').n_unique()" for col in ['total_sku_count']]
            
        # uniq_cols = [f"pl.col('total_sku_count').n_unique()"]
        columns_to_filter =[]
        values_to_filter = []
        selected_row = data_filter['expand']["row"]
        # Apply secondary filters and update filter_condition, sub_filter_state, and group
        filter_condition,sub_filter_state,group = OTB.secondary_filter(DATA,filters,sub_filter_state,group,filter_condition)
        #Apply hierachical filters
        for i in selected_row:
            if i in HEIRARCHY:
                columns_to_filter.append(i)
                values_to_filter.append(selected_row[i])
                last_filter = HEIRARCHY.index(columns_to_filter[0])
        
        group_to_filter = []
        group_value_to_filter = []
        for i in selected_row:
            if i in group:
                group_to_filter.append(i)
                group_value_to_filter.append(selected_row[i])
        
        
        for col, val in zip(columns_to_filter+group_to_filter, values_to_filter+group_value_to_filter):
            if filter_condition is None:
                try:
                    filter_condition = (DATA[col] == val)
                except Exception as e:
                    print(f"wrong heirarchy: {e}")
            else:
                filter_condition = filter_condition & (DATA[col] == val)

        if filter_condition is not None:

            # Add filter_index column based on filter_condition
            DATA = DATA.with_columns(filter_index=filter_condition)
            DATA = DATA.with_columns(filter_index= pl.col("filter_index").cast(pl.Float64))
            print(DATA.columns,"FILTERDATA")
        if columns_to_filter == []:
            print('we are finding corr ex2')

            #form subset for calculation 
            last_filter = 0
            group.append(HEIRARCHY[last_filter])
            group = list(set(group))
            print(group,"FILTERDATA1")
            print(filter_condition, 'filter_cond')

            # item_group = group + ["ITEMID"]
            # item_group = list(set(item_group).intersection(Budget.group_subset))
            if sub_filter_state:
                try:
                    # do initial calculation and aggregation
                    # datas = Budget.calculate_initial_frame(DATA.filter(list(filter_condition)),item_group)
                    # sum_cols = [sum_cols.remove(x) for x in sum_cols if x.replace('sum()', '') == x.replace('n_unique()', '')]
                    agg_dict = [eval(expr) for expr in list(mean_cols)+sums_cols+rank_cols+dist_cols+art_cols+cumulatives_1+cumulatives_2] # +uniq_cols
                    if filter_condition is not None:

                        # Add filter_index column based on filter_condition
                        DATA = DATA.with_columns(filter_index=filter_condition)
                        DATA = DATA.with_columns(filter_index= pl.col("filter_index").cast(pl.Int8))
                    
                    filter_condition = DATA["filter_index"]==1
                    data = DATA.filter(list(filter_condition)).group_by(list(set(group)), maintain_order = True).agg(agg_dict) # , maintain_order = True
                    print(DATA.filter(list(filter_condition)).columns,"FILTERDATA3")

                    #calculate subfilters options
                    # Budget.SUB_FILTER = Budget.call_filter(datas.filter(list(filter_condition)),Budget.SUB_FILTER,group,DATA,Budget.filter_store)
                except:
                    print(traceback.format_exc())  
            else:
                print('we are finding corr ex3')

                #do initial calculation and aggregation
                # datas = Budget.calculate_initial_frame(DATA,item_group)
                # sum_cols = [sum_cols.remove(x) for x in sum_cols if x.replace('sum()', '') == x.replace('n_unique()', '')]
                agg_dict = [eval(expr) for expr in list(mean_cols)+sums_cols+rank_cols+dist_cols+art_cols+cumulatives_1+cumulatives_2] #+uniq_cols
                print(group, 'gp_no_SFS_exp')
                print(agg_dict)
                if filter_condition is not None:

                    # Add filter_index column based on filter_condition
                    DATA = DATA.with_columns(filter_index=filter_condition)
                    DATA = DATA.with_columns(filter_index= pl.col("filter_index").cast(pl.Float64))

                    filter_condition = DATA["filter_index"]==1
                print(group,"FILTERDATA5")

                data = DATA.group_by(list(set(group)),  maintain_order = True).agg(agg_dict) # 
                # for name, dat in data:
                #     print(name)
                #     print(dat.select(pl.col(['Channel', 'ProposedSellThru%', 'PurchaseRetailValueatGrossSale', 'TYForecast', 'OTBorPurchaseCost', 'PurchasedRetailValueGrossSale'])), 'xxx')
                
                print(data.select(pl.col(['Channel', 'COR_EOLStock_value', 'PurchaseRetailValueatGrossSale', 'PurchasedRetailValueGrossSale', 'ProposedSellThru%', 'initial_average_retail_price', 'Markdown%', 'TYForecast', 'OTBorPurchaseCost']).sum()))
                # print(DATA.select())
                #calculate subfilters options
                # Budget.SUB_FILTER = Budget.call_filter(datas,Budget.SUB_FILTER,group,DATA,Budget.filter_store)    
        else:
            print('we are finding corr ex4')

            try:
                groups = max_col[limit:max_col.index(columns_to_filter[0])+1]
            except:
                groups = max_col[:max_col.index(columns_to_filter[0])+1]
            group.append(HEIRARCHY[last_filter+1])

            #form subset for calculation
            # item_group = group+["ITEMID"]
            # item_group = list(set(item_group + HEIRARCHY[:last_filter+1]))
            # item_group = list(set(item_group).intersection(Budget.group_subset))
            if filter_condition is not None:

                # Add filter_index column based on filter_condition
                DATA = DATA.with_columns(filter_index=filter_condition)
                DATA = DATA.with_columns(filter_index= pl.col("filter_index").cast(pl.Float64))

            

            #do initial calculation and aggregation
            
            # datas = Budget.calculate_initial_frame(DATA.filter(list(filter_condition)),item_group)
            maxs_cols = [f"pl.col('{col}').max()" for col in groups if col not in group]
            agg_dict = [eval(expr) for expr in mean_cols+sums_cols+maxs_cols+rank_cols+dist_cols+art_cols+cumulatives_1+cumulatives_2] # uniq_cols
            
            
            filter_condition = DATA["filter_index"]==1
            try:
                data = DATA.filter(list(filter_condition)).group_by(list(set(group)), maintain_order =True).agg(agg_dict) #, , maintain_order = True 
            except:
                print(traceback.format_exc())
            #calculate subfilters options
            # Budget.SUB_FILTER = Budget.call_filter(datas.filter(list(filter_condition)),Budget.SUB_FILTER,group,DATA,Budget.filter_store)
        # print(DATA.columns, 'in expand___')
        return data, filter_condition, group
    
   
    def sort_and_clean(self, data_filter:Dict, data:pl.DataFrame, filters:Filters):
        """
        Prepare data for display in UI,do sorting/pagination/cleaning.

        Args:
            data_filter (Dict): input response dict.
            data (pl.DataFrame): DataFrame to be sorted and cleaned and paginated.

        Returns:
            pl.DataFrame: Sorted ,cleaned and paginated DataFrame.

        """
        print('we are finding corr ex5')

        try:
            sort_columnid = data_filter['sort']['id']
            print(sort_columnid,"SORTID")
            # Check if sorting order is ascending or descending and apply sorting
            if data_filter['sort']['desc'] == False:
                datas = data.sort(by =[sort_columnid], descending= False, maintain_order = True)[filters.page_number*filters.page_size:(filters.page_number+1)*filters.page_size] #, maintain_order = True
            elif data_filter['sort']['desc'] == True:
                datas =data.sort(by =[sort_columnid], descending= True, maintain_order = True)[filters.page_number*filters.page_size:(filters.page_number+1)*filters.page_size]    #, maintain_order = True
        except:
            # If there is an exception, fallback to default pagination
            datas = data[(filters.page_number*filters.page_size):((filters.page_number+1)*filters.page_size)]
            
        datas = datas.to_pandas()
        datas[float_cols] = datas[float_cols].replace([np.nan,np.inf,-np.inf],0).astype(float).round(2)
        datas[int_cols]   = datas[int_cols].replace([np.nan,np.inf,-np.inf],0).astype(int)

        return datas