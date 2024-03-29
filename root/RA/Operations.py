import polars as pl
import numpy as np

import traceback
import pandas as pd

from .parameters import Parameters,Budget
from .schemas import Filters
from typing import Dict,List,Optional,Union,Tuple

Budget = Budget()
gloabal_vars = Parameters()


class Operations:
    def user_input(self,data_filter:Dict,DATA:pl.DataFrame) ->pl.DataFrame:

        """
        This function takes input data response dict and a DataFrame, performs various calculations, and returns an updated DataFrame.

        Args:
            data_filter (Dict): A di
            ctionary containing input parameters for the calculations.
            DATA (pl.DataFrame): The input DataFrame.

        Returns:
            pl.DataFrame: The updated DataFrame after performing calculations.

        """

        item_grouped_df = pl.DataFrame()
        grouped_df = pl.DataFrame()

        desired_st_percent = data_filter["input_field"]["desired_st%"]/100
        desired_growth_percent = data_filter["input_field"]["desired_growth%"]/100
        desired_markdown_percent = data_filter["input_field"]["desired_markdown%"]/100
        tail_percent = data_filter["input_field"]["tail%>"]/100
        st_70_Percent_mix = data_filter["input_field"]["st>70%mix"]/100
        top_normalpercent_Mix = data_filter["input_field"]["top&normal%mix"]/100
        forecast_net_sales = data_filter["input_field"]["forecast_net_sales"]
        forecast_sku = data_filter["input_field"]["forecast_sku"]



        try:
            #calculate plan_for_otb_value
            if desired_st_percent >0:
                item_grouped_df = Budget.get_item_data(DATA,{"gross_sales_ly":1},True)
                newValue = item_grouped_df["gross_sales_ly"].sum()/ desired_st_percent 
                print(newValue,"nes")
                DATA = Budget.drill_down(DATA,"plan_for_otb_value",newValue,"gross_sales_ly")
            else:
                item_grouped_df = Budget.get_item_data(DATA,{"gross_sales_ly":1},True)


            #calculate plan_for_net_sales_value
            plan_otb_value = DATA["plan_for_otb_value"].sum()
            newValue = (plan_otb_value * desired_st_percent) *(1-desired_markdown_percent)*(1+desired_growth_percent)
            DATA = Budget.drill_down(DATA,"plan_for_net_sales_value",newValue,"plan_for_otb_value")

            #calculate plan_for_net_sales_mix_percent and normalised_sales_mix_final_percent
            sums = DATA["plan_for_net_sales_value"].sum()
            DATA = DATA.with_columns(((DATA["plan_for_net_sales_value"]/sums)*100).alias("plan_for_net_sales_mix_percent"))
            DATA = DATA.with_columns((DATA["plan_for_net_sales_mix_percent"]).alias("normalised_sales_mix_final_percent"))

            #calculate plan_for_sku_article_count
            df_article = Budget.calculate_initial_frame(DATA,["ITEMID"])
            df_counts = pl.DataFrame()
            newValue = 0
            cols = []

            article_maps = {"tail":{'ref_col':'cum_sum','percent':tail_percent},"st_greater_than_70":{'ref_col':"sellthrough_percent",'percent':st_70_Percent_mix},"top_and_normal":{'ref_col':'cum_sum','percent':top_normalpercent_Mix}}
            for col,ref in article_maps.items():
                cols.append(col)
                counts_df = Budget.calculate_article_count(df_article,ref['percent'],col,ref['ref_col'])
                df_counts = pl.concat([df_counts,counts_df])

            newValue = newValue + len(df_counts.unique())
            if newValue != 0:
                DATA = Budget.drill_down_multi(DATA,"plan_for_sku_article_count",newValue,cols)
            else:
                DATA = DATA.with_columns(pl.lit(0).alias("plan_for_sku_article_count"))
    
            #calculate plan_for_net_qty
            mean = item_grouped_df["final_price"].mean()
            newValue = DATA["plan_for_net_sales_value"].sum() / item_grouped_df["final_price"].mean() if mean !=0 else 0
            DATA = Budget.drill_down(DATA,"plan_for_net_qty",newValue,"plan_for_sku_article_count")

            #calculate plan_for_purchase_qty
            mean = item_grouped_df["initial_average_retail_price"].mean()
            newValue = DATA["plan_for_otb_value"].sum()/mean if mean !=0 else 0
            DATA = Budget.drill_down(DATA,"plan_for_purchase_qty",newValue,"plan_for_net_qty")

            #calculate normalised_sku_article_mix_final_percent and normalised_sku_article_mix_final_percent
            sums = item_grouped_df["net_sales_mix_percent"].sum()
            sum2 = item_grouped_df["purchase_value_mix_percent"].sum()
            newValue = sums *(sums/sum2) if sum2 !=0 else 0
            DATA = Budget.drill_down(DATA,"normalised_sku_article_mix_final_percent",newValue,"plan_for_sku_article_count")
            DATA = DATA.with_columns((DATA["normalised_sku_article_mix_final_percent"].replace({np.inf:0,-np.inf:0}).fill_nan(0)).alias("normalised_sku_article_mix_final_percent"))

            #calculate fcst_net_sales_value and fcst_net_sales_value_sales_mix_percent
            newValue = forecast_net_sales * (DATA["normalised_sales_mix_final_percent"].sum()/100)
            DATA = Budget.drill_down(DATA,"fcst_net_sales_value",newValue,"plan_for_net_sales_value")
            sums = item_grouped_df["fcst_net_sales_value"].sum()
            DATA = DATA.with_columns(((DATA["fcst_net_sales_value"]/sums)*100).alias("fcst_net_sales_value_sales_mix_percent"))

            #calculate fcst_sku_article and fcst_sku_article_mix_percent
            newValue = forecast_sku * (DATA["normalised_sku_article_mix_final_percent"].sum()/100)
            DATA = Budget.drill_down(DATA,"fcst_sku_article",newValue,"plan_for_sku_article_count")
            sums = DATA["fcst_sku_article"].sum()
            DATA = DATA.with_columns(((DATA["fcst_sku_article"]/sums)*100).alias("fcst_sku_article_mix_percent"))

            #calculate fcst_otb_value
            if desired_st_percent >0:
                newValue = forecast_net_sales/desired_st_percent
            DATA = Budget.drill_down(DATA,"fcst_otb_value",newValue,"fcst_sku_article")
    
            #calculate fcst_otb_value_at_cost
            item_grouped_df = item_grouped_df.with_columns( (item_grouped_df["initial_gp_percent"].replace({np.inf:0,-np.inf:0}).fill_nan(0)).alias("initial_gp_percent"))
            newValue = DATA["fcst_otb_value"].sum() *(1- (item_grouped_df["initial_gp_percent"].mean()/100))
            DATA = Budget.drill_down(DATA,"fcst_otb_value_at_cost",newValue,"fcst_otb_value")

            #calculate fcst_otb_quantity
            mean = item_grouped_df["initial_average_retail_price"].mean()
            newValue = DATA["fcst_net_sales_value"].sum()/mean if mean !=0 else 0
            DATA = Budget.drill_down(DATA,"fcst_otb_quantity",newValue,"fcst_sku_article")
            return DATA
            
        except Exception as e:
            print(traceback.format_exc())


    def manage_check_box_selection(self,DATA:pl.DataFrame,columnID:str,newValue:int,data_filter:Dict,parent:[Optional]=None,child:[Optional]=None,
                                    other_filter_condition:[Optional]=None,filter_condition:[Optional]=None) ->pl.DataFrame:

        row = data_filter["table_changes"]["row"]

        if columnID == 'Check_box' and newValue == 1:
            print('we are in check box')
            
            if child is None: #and parent == None:
                DATA = DATA.with_columns(Check_box = pl.lit(newValue).cast(pl.Int8))
                DATA = DATA.with_columns(renewed_budget_percent = pl.col('coefficient_score_mix_percent').cast(pl.Float64))
                DATA = DATA.with_columns(renewed_budget_amount = (pl.col('budget_amount')*pl.col('budget_percent'))/100)
                DATA = DATA.with_columns(different_bud_perc = (pl.col('renewed_budget_percent'))-(pl.col('budget_percent')))
                DATA = DATA.with_columns(re_assigned_mix = pl.lit(0).cast(pl.Float64))
                DATA = DATA.with_columns(new_budget_mix = pl.col('budget_percent') - pl.col('re_assigned_mix'))
                DATA = DATA.with_columns(revised_budget_amount = pl.col('budget_amount').sum() * pl.col('budget_percent'))

            # if not child.is_empty():
            else:
                print('child is not empty')
                Data_major = DATA.filter(list(child))
                Data_major = Data_major.with_columns(Check_box = pl.lit(newValue).cast(pl.Int8))
                # print(Data_major['budget_percent'].value_counts())
                sums = row['budget_amount']
                Data_major = Data_major.with_columns(renewed_budget_percent = pl.col('coefficient_score_mix_percent').cast(pl.Float64))
                Data_major = Data_major.with_columns(renewed_budget_amount = (pl.col('budget_amount')*pl.col('budget_percent'))/100)
                Data_major = Data_major.with_columns(different_bud_perc = (pl.col('renewed_budget_percent'))-(pl.col('budget_percent')))
                Data_major = Data_major.with_columns(re_assigned_mix = pl.lit(0).cast(pl.Float64))
                # perc = (amnt/total)*100
                Diff_sum_mix = Data_major['different_bud_perc'].sum()
                # print(Data_major.shape)
                # print(Data_major['budget_percent'].value_counts())

                if Data_major.select(pl.col('coefficient_score_mix_percent').cast(pl.Int8)).equals(Data_major.select(pl.col('budget_percent').cast(pl.Int8))):
                    print('budget col and coeff scoremix are equal')
                Data_minor = DATA.filter(list(child.not_()))
                Data_minor = Data_minor.with_columns(Check_box = pl.lit(0).cast(pl.Int8))
                Data_minor = Data_minor.with_columns(renewed_budget_percent = pl.col('budget_percent').cast(pl.Float64))
                Data_minor = Data_minor.with_columns(renewed_budget_amount = (pl.col('budget_amount')*pl.col('budget_percent'))/100)
                Data_minor = Data_minor.with_columns(different_bud_perc = (pl.col('renewed_budget_percent'))-(pl.col('budget_percent')))
                budget_perc_sum_ = Data_minor['budget_percent'].sum()
                Data_minor = Data_minor.with_columns(re_assigned_mix = (pl.col('different_bud_perc').sum()) * pl.col('budget_percent')/pl.col('budget_percent').sum())

                # print(Data_minor['budget_percent'].value_counts())

                DATA = pl.concat([Data_major, Data_minor])
                DATA = DATA.with_columns(new_budget_mix = pl.col('budget_percent') - pl.col('re_assigned_mix'))
                DATA = DATA.with_columns(revised_budget_amount = pl.col('budget_amount').sum() * pl.col('budget_percent'))

        elif columnID == 'Check_box' and newValue == 0:
        #There is parent when unselected while there is historical year 
            if parent == None:
                DATA = DATA.with_columns(Check_box = pl.lit(newValue).cast(pl.Int8))
                DATA = DATA.drop(['renewed_budget_percent', 'renewed_budget_amount', 'different_bud_perc', 're_assigned_mix', 'new_budget_mix', 'revised_budget_amount'])
            
            elif not child.is_empty() or not parent.is_empty():
                Data_minor = DATA.filter(list(child)) 
                Data_minor = Data_minor.with_columns(Check_box = pl.lit(newValue).cast(pl.Int8))
                Data_minor = Data_minor.drop(['renewed_budget_percent', 'renewed_budget_amount', 'different_bud_perc', 're_assigned_mix', 'new_budget_mix', 'revised_budget_amount'])
                # print(Data_minor.shape, Data_minor.columns.to_list(), 'unnnn')
                Data_major = DATA.filter(list(child.not_()))
                Data_major = Data_major.with_columns(Check_box = pl.lit(0).cast(pl.Int8))
                Data_major = Data_major.drop(['renewed_budget_percent', 'renewed_budget_amount', 'different_bud_perc', 're_assigned_mix', 'new_budget_mix', 'revised_budget_amount'])
                DATA = pl.concat([Data_minor, Data_major])

            Data_major = DATA.filter(pl.col('Check_box') == 1)
            Data_major = Data_major.with_columns(budget_percent = pl.col('coefficient_score_mix_percent'))
            Data_minor = DATA.filter(pl.col('Check_box') == 0)
            DATA = pl.concat([Data_major, Data_minor])
        return DATA
        


    def edit_tables(self,DATA:pl.DataFrame,data:pl.DataFrame,row:Dict,group:List,newValue:int,columnID:str,columns_to_filter:List,sub_filter_state:bool,
                    parent:[Optional]=None,child:[Optional]=None,other_filter_condition:[Optional]=None,filter_condition:[Optional]=None) ->pl.DataFrame:
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

        #calculate differance between old and new value
        original = row[columnID]
        if row[columnID] == None:
            original = 0
        increase = newValue - original  

        #calculate budget_percent using act_forecast_vs_budget_percent and goto column budget_percent
        if columnID == "act_forecast_vs_budget_percent":
            
            budget_amount = (newValue*row["Sales Act/Forecast"])/100
            if parent is None: budget_amount_sum = DATA["budget_amount"].sum()

            else: budget_amount_sum = DATA.filter(list(parent))["budget_amount"].sum()
            newValue = (100*budget_amount)/budget_amount_sum
            columnID = "budget_percent"
            
        if columnID == "budget_amount":
            if child is None:
                DATA = DATA.with_columns(budget_amount = (DATA["budget_percent"] * newValue)/100)

        #calculate budget_percent/budget_amount using budget_vppy and goto column budget_percent/budget_amount
        if columnID == "budget_vppy":
            print('parent is', parent)
            print('child is', child)

            if (len(columns_to_filter)) ==1 and ((sub_filter_state ==True)):
                DATA = DATA.with_columns(((DATA["net_sales_lly"] * newValue)/100).alias("budget_amount"))
            elif child is None:
                DATA = DATA.with_columns(((DATA["net_sales_lly"] * newValue)/100).alias("budget_amount"))
                columnID == "budget_amount"
            else:
                budget_amount = (row["net_sales_lly"] * newValue)/100
                if parent is None: budget_amount_sum = DATA["budget_amount"].sum()
                else: budget_amount_sum = DATA.filter(list(parent))["budget_amount"].sum()
                if budget_amount < budget_amount_sum:
                    newValue = (100*budget_amount)/budget_amount_sum
                    columnID = "budget_percent"

        #calculate budget_percent using units_per_sku and goto column budget_percent
        if columnID == "units_per_sku": 
            budget_amount = (newValue * row["initial_average_retail_price"]*row["total_sku_count"])
            if parent is None: budget_amount_sum = DATA["budget_amount"].sum()
            else: budget_amount_sum = DATA.filter(list(parent))["budget_amount"].sum()
            if budget_amount < budget_amount_sum:
                newValue = (100*budget_amount)/budget_amount_sum
                columnID = "budget_percent"

        #calculate budget_percent using budget_per_sku and goto column budget_percent
        if columnID == "budget_per_sku":
            budget_amount = newValue * row["total_sku_count"]
            if parent is None: budget_amount_sum = DATA["budget_amount"].sum()
            else: budget_amount_sum = DATA.filter(list(parent))["budget_amount"].sum()

            if budget_amount < budget_amount_sum:
                newValue = (100*budget_amount)/budget_amount_sum
                columnID = "budget_percent"
                
        #calculate budget_percent using budget_qty and goto column budget_percent
        if columnID == "budget_qty":
            budget_amount = newValue * row["initial_average_retail_price"]
            if parent is None: budget_amount_sum = DATA["budget_amount"].sum()
            else: budget_amount_sum = DATA.filter(list(parent))["budget_amount"].sum()
            if budget_amount < budget_amount_sum:
                newValue = (100*budget_amount)/budget_amount_sum
                columnID = "budget_percent"  

        #distribute adjusted_budget_gross_margin_percent input value
        if columnID == "adjusted_budget_gross_margin_percent":
            DATA_child = Budget.drill_down_percentage(DATA.filter(list(child)),"adjusted_budget_gross_margin_percent",newValue,"budget_gross_margin_percent")
            DATA_siblings = DATA.filter(list(child.not_()))
            DATA = pl.concat([DATA_siblings,DATA_child])
            
        #calculate budget_percent/budget_amount using budget_vppy and goto column budget_percent/budget_amount
        if columnID == "budget_vpy":  
            if (len(columns_to_filter)) ==1 and ((sub_filter_state ==True)):
                DATA = DATA.with_columns(((DATA["net_sales_ly"] * newValue)/100).alias("budget_amount"))
            elif child is None:
                DATA = DATA.with_columns(((DATA["net_sales_ly"] * newValue)/100).alias("budget_amount"))
                columnID == "budget_amount"

            else:
                budget_amount = (row["net_sales_ly"] * newValue)/100
                if parent is None: budget_amount_sum = DATA["budget_amount"].sum()
                else: budget_amount_sum = DATA.filter(list(parent))["budget_amount"].sum()
                if budget_amount < budget_amount_sum:
                    newValue = (100*budget_amount)/budget_amount_sum
                    columnID = "budget_percent"

        #distribute normalised_sales_mix_final_percent input value
        if columnID == "normalised_sales_mix_final_percent":        
            if len(columns_to_filter) == 1:
                DATA = Budget.drill_input_editable(grouped_df=DATA.filter(list(child)),other_grouped_df=DATA.filter(list(other_filter_condition)),increase= increase, colID= columnID,ref_col="budget_amount")

            else:
                group_sum = DATA.filter(list(parent)).group_by(columns_to_filter).agg(pl.col("normalised_sales_mix_final_percent").sum())["normalised_sales_mix_final_percent"].sum()
                increase =  (group_sum * increase)/100
                DATA_parent = Budget.drill_input_editable(grouped_df=DATA.filter(list(child)),other_grouped_df=DATA.filter(list(other_filter_condition)),increase= increase, colID= columnID,ref_col="budget_amount")
                DATA_siblings = DATA.filter(list(parent.not_()))
                DATA = pl.concat([DATA_siblings,DATA_parent])

        #distribute normalised_sku_article_mix_final_percent input value
        if columnID == "normalised_sku_article_mix_final_percent":
            if len(columns_to_filter) == 1:
                DATA = Budget.drill_input_editable(grouped_df=DATA.filter(list(child)),other_grouped_df=DATA.filter(list(other_filter_condition)),increase= increase, colID= columnID,ref_col="total_sku_count")
                
            else:
                group_sum = DATA.filter(list(child)).group_by(columns_to_filter).agg(pl.col("normalised_sku_article_mix_final_percent").sum())["normalised_sku_article_mix_final_percent"].sum()
                group_sum = DATA.filter(list(other_filter_condition)).group_by(columns_to_filter).agg(pl.col("normalised_sku_article_mix_final_percent").sum())["normalised_sku_article_mix_final_percent"].sum()
                increase =  (group_sum * increase)/100
                DATA_parent = Budget.drill_input_editable(grouped_df=DATA.filter(list(child)),other_grouped_df=DATA.filter(list(other_filter_condition)),increase= increase, colID= columnID,ref_col="total_sku_count")
                DATA_siblings = DATA.filter(list(parent.not_()))
                DATA = pl.concat([DATA_parent,DATA_siblings])
        
        #distribute change in budget_percent
        if columnID == 'budget_percent':
            s = data['relative_budget_percent'].sum()
            increase = (s*newValue/100) - (s*row[columnID]/100)

            if sub_filter_state ==True:
                DATA = DATA.with_columns(((DATA['budget_percent']/DATA['budget_percent'].sum())*100).alias('budget_percent'))
                DATA = DATA.with_columns(DATA['budget_percent'].alias("relative_budget_percent"))

                DATA_parent = Budget.change_percent(grouped_df=DATA.filter(list(child)),other_grouped_df=DATA.filter(list(other_filter_condition)),increase= increase, colID= columnID)
                DATA_siblings = DATA.filter(list(parent.not_()))
                DATA = pl.concat([DATA_parent,DATA_siblings])
            
            else:
                if len(columns_to_filter) == 1:
                    DATA = Budget.change_percent(grouped_df=DATA.filter(list(child)),other_grouped_df=DATA.filter(list(other_filter_condition)),increase= increase, colID= columnID)

                else:
                    DATA_parent = Budget.change_percent(grouped_df=DATA.filter(list(child)),other_grouped_df=DATA.filter(list(other_filter_condition)),increase= increase, colID= columnID)
                    DATA_siblings = DATA.filter(list(parent.not_()))
                    DATA = pl.concat([DATA_siblings,DATA_parent])

        #distribute change in budget_amount
        elif columnID != None and columnID not in ["normalised_sales_mix_final_percent","normalised_sku_article_mix_final_percent"] :

            agg_dict = [eval(expr) for expr in 
                        [f"pl.col('{col}').mean()" for col in gloabal_vars.avg_col]+[f"pl.col('{col}').sum()" for col in gloabal_vars.sum_col]+[f"pl.col('{col}').max()" for col in gloabal_vars.rank_col]]

            if child is None:
                data = DATA.select(agg_dict)
            else:
                if sub_filter_state == True:

                    if len(columns_to_filter) == 1:
                        sibling_condition = child.not_()
                        DATA = Budget.change_percent(grouped_df=DATA.filter(list(child)),other_grouped_df=DATA.filter(list(sibling_condition)),increase= increase, colID= columnID)
                        data = DATA.filter(list(filter_condition)).group_by(list(set(group))).agg(agg_dict)

                    else:
                        DATA_parent = Budget.change_percent(grouped_df=DATA.filter(list(child)),other_grouped_df=DATA.filter(list(other_filter_condition)),increase= increase, colID= columnID)
                        DATA_siblings = DATA.filter(list(parent.not_()))
                        DATA = pl.concat([DATA_siblings,DATA_parent])
                        summation = DATA["budget_amount"].sum()
                        DATA = DATA.with_columns(((DATA["budget_amount"]/summation)*100).alias('budget_percent'))
            
                else:
                    if len(columns_to_filter) == 1:
                        DATA = Budget.change_percent(grouped_df=DATA.filter(list(child)),other_grouped_df=DATA.filter(list(other_filter_condition)),increase= increase, colID= columnID)
                        summation = DATA["budget_amount"].sum()
                        DATA = DATA.with_columns(((DATA["budget_amount"]/summation)*100).alias('budget_percent'))
                    else:
                        DATA_parent = Budget.change_percent(grouped_df=DATA.filter(list(child)),other_grouped_df=DATA.filter(list(other_filter_condition)),increase= increase, colID= columnID)
                        DATA_siblings = DATA.filter(list(parent.not_()))
                        DATA = pl.concat([DATA_siblings,DATA_parent])
                        summation = DATA["budget_amount"].sum()
                        DATA = DATA.with_columns(((DATA["budget_amount"]/summation)*100).alias('budget_percent'))

        #change all the columns associated with budget_percent for change in budget_percent
        if columnID == 'budget_percent':
            summation = DATA["budget_amount"].sum()
            DATA = DATA.with_columns(budget_amount = DATA[columnID]*summation/100)
            DATA = DATA.with_columns(((DATA['budget_amount']/DATA['budget_amount'].sum())*100).alias("relative_budget_percent"))
            DATA = DATA.with_columns(((pl.col('budget_amount')/pl.col("sales_actual"))*100).alias("act_forecast_vs_budget_percent"))
            DATA = DATA.with_columns(((pl.col('budget_amount')/pl.col("net_sales_ly"))*100).alias("budget_vpy"))
            DATA = DATA.with_columns(((pl.col('budget_amount')/pl.col("net_sales_lly"))*100).alias("budget_vppy"))
            DATA = DATA.with_columns(((pl.col('budget_amount')/pl.col("total_sku_count"))).alias("budget_per_sku"))
            DATA = DATA.with_columns(((pl.col("budget_amount")/pl.col("initial_average_retail_price")).round()).alias("budget_qty"))
            DATA = DATA.with_columns((pl.col("budget_qty")/pl.col("total_sku_count")).alias("units_per_sku"))
            DATA = DATA.with_columns((-((pl.col("budget_gross_margin_percent") * pl.col("budget_amount"))-(100 * pl.col("budget_amount")))/100).alias("budget_cost"))

        #change all the columns associated with normalised_sales_mix_final_percent for change in normalised_sales_mix_final_percent
        if columnID == "normalised_sales_mix_final_percent":
            summation = DATA["plan_for_net_sales_value"] .sum()
            DATA = DATA.with_columns(plan_for_net_sales_value = pl.col(columnID)*summation/100)
            DATA = DATA.with_columns(fcst_net_sales_value = pl.col("fcst_net_sales_value").sum()*(pl.col("normalised_sales_mix_final_percent")/pl.col("normalised_sales_mix_final_percent").sum()))
            DATA = DATA.with_columns(((pl.col("fcst_net_sales_value")/pl.col("fcst_net_sales_value").sum())*100).alias("fcst_net_sales_value_sales_mix_percent"))
        #change all the columns associated with normalised_sku_article_mix_final_percent for change in normalised_sku_article_mix_final_percent
        if columnID == "normalised_sku_article_mix_final_percent":
            DATA = DATA.with_columns((pl.col("fcst_sku_article").sum()*(pl.col("normalised_sku_article_mix_final_percent")/pl.col("normalised_sku_article_mix_final_percent").sum())).alias("fcst_sku_article"))
            DATA = DATA.with_columns(((pl.col("fcst_sku_article")/pl.col("fcst_sku_article").sum())*100).alias("fcst_sku_article_mix_percent"))

        return DATA,data
    
    def apply_group_by(self,DATA:pl.DataFrame,data:pl.DataFrame,data_filter:Dict,sub_filter_state:bool,group:List,filters:Filters,filter_condition:[Optional]=None) ->Union[None,pl.Series,pl.DataFrame]:
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

        limit = 0
        filter_condition  = None

        temp_cols = [f"pl.col('{col}').sum()" for col in set(['new_budget_mix', 'revised_budget_amount']).intersection(set(DATA.columns))
                        ]
        print(temp_cols,"sets")
        print([col for col in ['new_budget_mix', 'revised_budget_amount'] if col in DATA.columns],"vals")

        # Apply secondary filters and update filter_condition, sub_filter_state, and group
        filter_condition,sub_filter_state,group = Budget.apply_secondary_filters(DATA,filters,sub_filter_state,group,filter_condition)
        if filter_condition is not None:
            # Add filter_index column based on filter_condition
            DATA = DATA.with_columns(filter_index=filter_condition)
            DATA = DATA.with_columns(filter_index= pl.col("filter_index").cast(pl.Int8))

        if not len(data_filter["group_by"]["columns"]) == 0:
            for i in  data_filter['group_by']['columns']:
                if i in gloabal_vars.filter_details:
                    group.append(gloabal_vars.filter_details[i])

            limit = gloabal_vars.max_col.index(group[-1])
            agg_dict = [eval(expr) for expr in temp_cols+
                        [f"pl.col('{col}').mean()" for col in gloabal_vars.avg_col]+[f"pl.col('{col}').sum()" for col in gloabal_vars.sum_col]+
                        [f"pl.col('{col}').max()" for col in gloabal_vars.rank_col]]


            if filter_condition is not None:
                try:
                    filter_condition = DATA["filter_index"]==1
                    data = DATA.filter(list(filter_condition)).group_by(list(set(group))).agg(agg_dict)
                    #calculate subfilters options
                    Budget.SUB_FILTER = Budget.call_filter(DATA.filter(list(filter_condition)),Budget.SUB_FILTER,group,DATA,Budget.filter_store,data_filter)

                except Exception as e:
                    print(f"error is {e}")
            else:
                try:

                    data = DATA.group_by(list(set(group))).agg(agg_dict)
                    print(data, 'main filt')
                except:
                    print(f"error is {e}")
        else:
            rank_col = [f"pl.col('{col}').max()" for col in gloabal_vars.rank_col]
            mean_cols = [f"pl.col('{col}').mean()" for col in gloabal_vars.avg_col]
            sums_cols = [f"pl.col('{col}').sum()" for col in gloabal_vars.sum_col]



            if not sub_filter_state:
                try:
                     # Apply group-by without filter conditions
                    datas = Budget.calculate_initial_frame(DATA,["ITEMID"])
                    agg_dict = [eval(expr) for expr in temp_cols+list(mean_cols)+sums_cols+rank_col]
                    data = datas.select(agg_dict)
                    #calculate subfilters options
                    Budget.SUB_FILTER = Budget.call_filter(datas,Budget.SUB_FILTER,group,DATA,Budget.filter_store,data_filter)
                except:
                    pass
            else:
                try:
                    #form subset for calculation
                    item_group = group + ["ITEMID"]
                    item_group = list(set(item_group).intersection(Budget.group_subset))

                    #do initial calculation and aggregation
                    datas = Budget.calculate_initial_frame(DATA.filter(list(filter_condition)),item_group)
                    agg_dict = [eval(expr) for expr in temp_cols+list(mean_cols)+sums_cols]
                    filter_condition = datas["filter_index"]==1
                    data = datas.filter(list(filter_condition)).group_by(list(set(group))).agg(agg_dict)

                    #calculate subfilters options
                    Budget.SUB_FILTER = Budget.call_filter(datas.filter(list(filter_condition)),Budget.SUB_FILTER,group,DATA,Budget.filter_store,data_filter)

            
                except Exception as e:
                    print(traceback.format_exc())

        return data,filter_condition
    
 
 #1 optimization here
    def expand_hierarchy(self,DATA:pl.DataFrame,data_filter:Dict,sub_filter_state:bool,group:List,filters:Filters,filter_condition:[Optional]=None) ->Union[None,pl.Series,pl.DataFrame]:
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
        mean_cols = [f"pl.col('{col}').mean()" for col in gloabal_vars.avg_col]
        sums_cols = [f"pl.col('{col}').sum()" for col in gloabal_vars.sum_col]
        rank_col = [f"pl.col('{col}').max()" for col in gloabal_vars.rank_col]
        temp_cols = [f"pl.col('{col}').sum()" for col in set(['new_budget_mix', 'revised_budget_amount']).intersection(set(DATA.columns))
                        ]
        print(temp_cols)
        print([col for col in ['new_budget_mix', 'revised_budget_amount'] if col in DATA.columns],"vals")



        columns_to_filter =[]
        values_to_filter = []
        selected_row = data_filter['expand']["row"]
        filter_condition = None
        # Apply secondary filters and update filter_condition, sub_filter_state, and group
        filter_condition,sub_filter_state,group = Budget.apply_secondary_filters(DATA,filters,sub_filter_state,group,filter_condition)

        #Apply hierachical filters
        
        for i in selected_row:
            if i in gloabal_vars.heirarchy:
                columns_to_filter.append(i)
                values_to_filter.append(selected_row[i])
                last_filter = gloabal_vars.heirarchy.index(columns_to_filter[0])
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
            DATA = DATA.with_columns(filter_index= pl.col("filter_index").cast(pl.Int8))

        if columns_to_filter == []:

            #form subset for calculation 
            last_filter = 0
            group.append(gloabal_vars.heirarchy[last_filter])
            group = list(set(group))
            item_group = group + ["ITEMID"]
            item_group = list(set(item_group).intersection(Budget.group_subset))
            agg_dict = [eval(expr) for expr in temp_cols+list(mean_cols)+sums_cols+rank_col]
            if sub_filter_state:
                try:
                    #do initial calculation and aggregation
                    datas = Budget.calculate_initial_frame(DATA.filter(list(filter_condition)),item_group)
                    filter_condition = datas["filter_index"]==1
                    data = datas.filter(list(filter_condition)).group_by(list(set(group))).agg(agg_dict)

                    #calculate subfilters options
                    Budget.SUB_FILTER = Budget.call_filter(datas.filter(list(filter_condition)),Budget.SUB_FILTER,group,DATA,Budget.filter_store,data_filter)
                except:
                    print(traceback.format_exc())  
            else:
                #do initial calculation and aggregation
                datas = Budget.calculate_initial_frame(DATA,item_group)
                data = datas.group_by(list(set(group))).agg(agg_dict)

                #calculate subfilters options
                Budget.SUB_FILTER = Budget.call_filter(datas,Budget.SUB_FILTER,group,DATA,Budget.filter_store,data_filter)    
        else:
            try:
                groups = gloabal_vars.max_col[limit:gloabal_vars.max_col.index(columns_to_filter[0])+1]
            except:
                groups = gloabal_vars.max_col[:gloabal_vars.max_col.index(columns_to_filter[0])+1]
            group.append(gloabal_vars.heirarchy[last_filter+1])

            #form subset for calculation
            item_group = group+["ITEMID"]
            item_group = list(set(item_group + gloabal_vars.heirarchy[:last_filter+1]))
            item_group = list(set(item_group).intersection(Budget.group_subset))
            filter_condition = DATA["filter_index"]==1

            #do initial calculation and aggregation
            datas = Budget.calculate_initial_frame(DATA.filter(list(filter_condition)),item_group)
            maxs_cols = [f"pl.col('{col}').max()" for col in groups if col not in group]
            agg_dict = [eval(expr) for expr in temp_cols+mean_cols+sums_cols+maxs_cols+rank_col]
            filter_condition = datas["filter_index"]==1
            data = datas.filter(list(filter_condition)).group_by(list(set(group))).agg(agg_dict)
            #calculate subfilters options
            Budget.SUB_FILTER = Budget.call_filter(datas.filter(list(filter_condition)),Budget.SUB_FILTER,group,DATA,Budget.filter_store,data_filter)
        return DATA,data,filter_condition


    def find_total_row(self,data:pl.DataFrame,dtemp:pl.DataFrame,channel_flag:bool,data_filter:Dict) ->Tuple[pd.Series,pl.DataFrame,bool]:
        '''
        Find the total row for the specified DataFrame and filters.

        Args:
            data (pl.DataFrame): Main DataFrame containing the data.
            dtemp (pl.DataFrame): Temporary DataFrame for bottom row calculations.
            channel_flag (bool): Flag indicating channel level/second level.
            data_filter (Dict): Dictionary containing user-specified filters.

        Returns:
            Tuple[pd.Series, pl.DataFrame, bool]: Total row, modified DataFrame, and channel flag.
        '''
        agg_dict = [
            eval(expr) for expr in 
            [f"pl.col('{col}').mean()" for col in gloabal_vars.avg_col] + 
            [f"pl.col('{col}').sum()" for col in gloabal_vars.sum_col]
        ]
        
        if channel_flag == False:
            bottom_column              = data.select(agg_dict)
            if type(bottom_column) != dict:
                bottom_column = bottom_column.to_dict()
                bottom_column = {key:bottom_column[key][0] for key in list(bottom_column.keys())}

        else:
            
            bottom_dict = {}
            temp_main_agg              = data.select(agg_dict)
            bottom_temp = dtemp.select(agg_dict).to_dict()

            bottom_temp = {key:bottom_temp[key][0] for key in list(bottom_temp.keys())}
            if data_filter['tab_name'] == 'raBudgetValue':
                for col in list(bottom_temp.keys()):
                    if col in gloabal_vars.non_article_cols:
                        bottom_temp[col] = temp_main_agg[col][0]
            bottom_column = bottom_temp
            if type(bottom_temp) != dict:
                bottom_column = bottom_column.to_dict()
                bottom_column = {key:bottom_column[key][0] for key in list(bottom_column.keys())}

        if data_filter['tab_name'] == 'raBudgetValue':
            bottom_column = Budget.calculate_bottom_first_tab(bottom_column)
        elif data_filter['tab_name'] == 'raBudgetMargin':
            bottom_column = Budget.calculate_bottom_margin_tab(bottom_column)
        elif data_filter['tab_name'] == 'raBudgetQuantity':
            bottom_column = Budget.calculate_bottom_cost_tab(bottom_column)
        elif data_filter['tab_name'] == 'raBudgetMargin': 
            bottom_column = Budget.calculate_bottom_qty_tab(bottom_column)
            
        bottom_column = pd.Series(bottom_column)
        bottom_column[gloabal_vars.int_cols]    = bottom_column[gloabal_vars.int_cols].replace([np.inf,-np.inf],0).fillna(0).astype(int)
        bottom_column[gloabal_vars.float_cols]  = bottom_column[gloabal_vars.float_cols].astype(float).round(2)
        return bottom_column,dtemp,channel_flag


    def sort_and_clean(self,data_filter:Dict,data:pl.DataFrame,filters:Filters) ->pl.DataFrame:
        """
        Prepare data for display in UI,do sorting/pagination/cleaning.

        Args:
            data_filter (Dict): input response dict.
            data (pl.DataFrame): DataFrame to be sorted and cleaned and paginated.

        Returns:
            pl.DataFrame: Sorted ,cleaned and paginated DataFrame.

        """
        try:
            sort_columnid = data_filter["sort"]["id"]
            # Check if sorting order is ascending or descending and apply sorting
            if data_filter["sort"]["desc"] == False:
                datas = data.sort(by =[sort_columnid], descending = False)[filters.page_number*filters.page_size:(filters.page_number+1)*filters.page_size]
            elif data_filter["sort"]["desc"] == True:
                datas = data.sort(by =[sort_columnid], descending = True)[filters.page_number*filters.page_size:(filters.page_number+1)*filters.page_size]
        except:
            # If there is an exception, fallback to default pagination
            datas = data[(filters.page_number*filters.page_size):((filters.page_number+1)*filters.page_size)]

        # Convert the sorted data to a Pandas DataFrame for further processing
        datas = datas.to_pandas()
        datas = datas.drop(["temp_purchase","temp_sold"],axis = 1)
        # clean the final data
        datas[gloabal_vars.float_cols] = datas[gloabal_vars.float_cols].replace([np.nan,np.inf,-np.inf],0).astype(float).round(2)
        datas[gloabal_vars.int_cols]   = datas[gloabal_vars.int_cols].replace([np.nan,np.inf,-np.inf],0).astype(int)
        for col in datas.columns:
            if col not in gloabal_vars.max_col:
                datas[col] = datas[col].fillna(0)
        return datas

    
    
